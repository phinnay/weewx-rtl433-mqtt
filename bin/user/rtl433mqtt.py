#!/usr/bin/env python3
# Distributed under the terms of the GNU Public License (GPLv3)
"""
weewx driver that consumes rtl_433 events from an MQTT broker.

Inspired by weewx-sdr (https://github.com/matthewwall/weewx-sdr).  Where
weewx-sdr spawns rtl_433 as a subprocess on the weewx host, this driver
expects rtl_433 to be running elsewhere and publishing JSON events to
MQTT via 'rtl_433 -F mqtt://<broker>:<port>,events'.  See the readme for
a full architecture diagram and step-by-step setup.

Configuration shape (full example - all options shown):

  [RTL433MQTT]
      driver = user.rtl433mqtt

      # broker connection
      host  = mqtt.local
      port  = 1883
      topic = rtl_433/+/events
      # username = weewx
      # password = SECRET
      # tls = false
      # client_id =

      # METRIC (default; rtl_433 mostly emits SI) or US.  controls usUnits
      # and triggers automatic suffix conversion (_C<->_F, _mm<->_in,
      # _km_h<->_mi_h, _kph<->_mph, _hPa<->_inHg) on incoming events.
      unit_system = METRIC

      # log_unmapped_sensors = false  # noisy but useful while building map
      # log_unknown_sensors  = false  # log payloads we cannot parse as JSON
      # log_packets          = true   # log every yielded packet (debug level)

      # sensor_map tuples are <rtl_433_field>.<sensor_id>.<rtl_433_model>.
      # use the exact strings rtl_433 publishes (id is decimal, model is
      # hyphenated like "Acurite-Tower").  fnmatch-style globs work in any
      # of the three components.
      [[sensor_map]]
          outTemp     = temperature_C.11041.Acurite-Tower
          outHumidity = humidity.11041.Acurite-Tower
          rain_total  = rain_mm.38.Fineoffset-WH1080

      # split cumulative counters (rain_total, strike_count) into deltas.
      # the right-hand side is the field NAME in the mapped packet.
      [[deltas]]
          rain                   = rain_total
          lightning_strike_count = strike_count

The sensor_map tuple format is intentionally NOT the same as weewx-sdr's
<obs>.<id>.<PacketClassName> syntax.  weewx-sdr maps to its normalized
field names; this driver maps to the raw rtl_433 fields, so the strings
differ - sensor maps cannot be copied between drivers without rewriting.

Standalone test mode:

  This module can be run directly to subscribe to a broker and dump the
  full parse -> sensor_map -> delta pipeline for each event.  Does not
  require weewx to be installed.  Most common forms:

      python3 rtl433mqtt.py --host BROKER             # parsed events only
      python3 rtl433mqtt.py --config /etc/weewx/weewx.conf  # incl. mapping

  See 'python3 rtl433mqtt.py --help' or the readme for the full set of
  flags and example invocations.
"""

from calendar import timegm
import fnmatch
import json
import queue
import time

try:
    import paho.mqtt.client as mqtt
except ImportError as e:
    raise ImportError(
        "paho-mqtt is required for user.rtl433mqtt: pip install paho-mqtt"
    ) from e

# weewx is hard-required when this module is loaded as a driver, but the
# standalone test mode (run as __main__) can work without it.  fall back to
# minimal stubs so 'python3 rtl433mqtt.py --config weewx.conf --host ...'
# works on a dev machine that doesn't have weewx installed.
try:
    import weewx
    import weewx.drivers
    from weeutil.weeutil import tobool
except ImportError:
    import types as _types
    weewx = _types.ModuleType('weewx')
    weewx.US = 1
    weewx.METRIC = 16
    class _StubIOError(Exception): pass
    weewx.WeeWxIOError = _StubIOError
    weewx.drivers = _types.ModuleType('weewx.drivers')
    class _StubAbstractDevice: pass
    class _StubAbstractConfEditor: pass
    weewx.drivers.AbstractDevice = _StubAbstractDevice
    weewx.drivers.AbstractConfEditor = _StubAbstractConfEditor

    def tobool(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        return str(v).strip().lower() in ('true', '1', 'yes', 'on')

try:
    import weeutil.logger
    import logging
    log = logging.getLogger(__name__)
    def logdbg(m): log.debug(m)
    def loginf(m): log.info(m)
    def logerr(m): log.error(m)
except ImportError:
    import syslog
    def _logmsg(level, m):
        syslog.syslog(level, 'rtl433mqtt: %s' % m)
    def logdbg(m): _logmsg(syslog.LOG_DEBUG, m)
    def loginf(m): _logmsg(syslog.LOG_INFO, m)
    def logerr(m): _logmsg(syslog.LOG_ERR, m)


DRIVER_NAME = 'RTL433MQTT'
DRIVER_VERSION = '0.1'

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 1883
DEFAULT_TOPIC = 'rtl_433/+/events'
DEFAULT_UNIT_SYSTEM = 'METRIC'

# suffix-based unit conversions.  applied after parsing every event so the
# user can map to a single field name regardless of what rtl_433 emitted.
# tuple is (source_suffix, target_suffix, conversion_lambda).
_US_FROM_METRIC = [
    ('_C',    '_F',    lambda v: v * 1.8 + 32),
    ('_mm',   '_in',   lambda v: v / 25.4),
    ('_km_h', '_mi_h', lambda v: v * 0.621371),
    ('_kph',  '_mph',  lambda v: v * 0.621371),
    ('_hPa',  '_inHg', lambda v: v * 0.02953),
]
_METRIC_FROM_US = [
    ('_F',    '_C',    lambda v: (v - 32) * 5 / 9),
    ('_in',   '_mm',   lambda v: v * 25.4),
    ('_mi_h', '_km_h', lambda v: v / 0.621371),
    ('_mph',  '_kph',  lambda v: v / 0.621371),
    ('_inHg', '_hPa',  lambda v: v / 0.02953),
]


def loader(config_dict, _):
    return RTL433MQTTDriver(**config_dict[DRIVER_NAME])


def confeditor_loader():
    return RTL433MQTTConfigurationEditor()


def _parse_time(s):
    if not s:
        return int(time.time())
    s = str(s).rstrip('Z').split('.')[0]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return timegm(time.strptime(s, fmt))
        except (TypeError, ValueError):
            continue
    logdbg("could not parse time '%s'; using now" % s)
    return int(time.time())


def _normalize_battery(obj):
    # weewx convention: 0 = ok, 1 = low.  rtl_433 modern emits battery_ok=1
    # for ok.  legacy used battery='OK' or battery_low=1.
    if 'battery_ok' in obj:
        try:
            v = float(obj['battery_ok'])
            return 0 if v >= 1.0 else 1
        except (TypeError, ValueError):
            pass
    if 'battery_low' in obj:
        try:
            return int(obj['battery_low'])
        except (TypeError, ValueError):
            pass
    if 'battery' in obj:
        return 0 if obj.get('battery') == 'OK' else 1
    return None


def _convert_units(pkt, unit_system):
    rules = _US_FROM_METRIC if unit_system == 'US' else _METRIC_FROM_US
    for src, tgt, fn in rules:
        for k in list(pkt.keys()):
            if not k.endswith(src):
                continue
            v = pkt.pop(k)
            if v is None:
                continue
            try:
                pkt[k[:-len(src)] + tgt] = fn(float(v))
            except (TypeError, ValueError):
                pass


def _parse_event(payload, unit_system):
    """Turn one rtl_433 mqtt event payload into a tagged loop packet dict.

    Returns None if the payload is not parseable JSON or has no model.
    """
    try:
        obj = json.loads(payload)
    except (ValueError, TypeError) as e:
        logdbg("could not parse json payload: %s" % e)
        return None
    if not isinstance(obj, dict) or 'model' not in obj:
        logdbg("event missing 'model' field; dropping")
        return None

    model = str(obj['model'])
    sensor_id = obj.get('id', obj.get('sensor_id', ''))
    sensor_id = str(sensor_id)

    pkt = {}
    skip = {'time', 'model', 'id', 'sensor_id'}
    for k, v in obj.items():
        if k not in skip:
            pkt[k] = v

    bs = _normalize_battery(obj)
    if bs is not None:
        pkt['battery'] = bs

    _convert_units(pkt, unit_system)

    out = {
        'dateTime': _parse_time(obj.get('time')),
        'usUnits': weewx.US if unit_system == 'US' else weewx.METRIC,
    }
    for k, v in pkt.items():
        out['%s.%s.%s' % (k, sensor_id, model)] = v
    return out


def _find_match(pattern, keylist):
    # glob-style match across the three-part tuple, mirroring weewx-sdr.
    if pattern in keylist:
        return pattern
    parts = pattern.split('.')
    if len(parts) != 3:
        return None
    for k in keylist:
        kparts = k.split('.')
        if (len(kparts) == 3
                and fnmatch.filter([kparts[0]], parts[0])
                and fnmatch.filter([kparts[1]], parts[1])
                and fnmatch.filter([kparts[2]], parts[2])):
            return k
    return None


class RTL433MQTTConfigurationEditor(weewx.drivers.AbstractConfEditor):
    @property
    def default_stanza(self):
        return """
[RTL433MQTT]
    # consume rtl_433 events from an MQTT broker
    driver = user.rtl433mqtt

    host = localhost
    port = 1883
    topic = rtl_433/+/events
    # username =
    # password =
    # tls = false
    # client_id =

    # METRIC (default) or US.  controls usUnits and triggers automatic
    # suffix-based conversion (_C<->_F, _mm<->_in, _km_h<->_mi_h, etc.)
    unit_system = METRIC

    # sensor_map tuples are <rtl_433_field>.<sensor_id>.<rtl_433_model>.
    # the field part uses the rtl_433 unit suffix matching unit_system
    # (e.g. temperature_C with METRIC, temperature_F with US).
    [[sensor_map]]
#        outTemp     = temperature_C.11041.Acurite-Tower
#        outHumidity = humidity.11041.Acurite-Tower

    # rain and lightning are cumulative on the sensor; deltas split them
    # into per-period values.
    [[deltas]]
        rain                   = rain_total
        lightning_strike_count = strike_count
"""


class RTL433MQTTDriver(weewx.drivers.AbstractDevice):

    DEFAULT_DELTAS = {
        'rain': 'rain_total',
        'lightning_strike_count': 'strike_count',
    }

    def __init__(self, **stn_dict):
        loginf('driver version is %s' % DRIVER_VERSION)
        self._model = stn_dict.get('model', 'RTL433MQTT')

        self._host = stn_dict.get('host', DEFAULT_HOST)
        self._port = int(stn_dict.get('port', DEFAULT_PORT))
        self._topic = stn_dict.get('topic', DEFAULT_TOPIC)
        self._username = stn_dict.get('username')
        self._password = stn_dict.get('password')
        self._tls = tobool(stn_dict.get('tls', False))
        self._client_id = stn_dict.get('client_id', '')

        self._unit_system = stn_dict.get(
            'unit_system', DEFAULT_UNIT_SYSTEM).upper()
        if self._unit_system not in ('US', 'METRIC'):
            loginf("unknown unit_system '%s'; falling back to METRIC"
                   % self._unit_system)
            self._unit_system = 'METRIC'

        self._sensor_map = stn_dict.get('sensor_map', {})
        self._deltas = stn_dict.get('deltas', RTL433MQTTDriver.DEFAULT_DELTAS)
        self._counter_values = {}

        self._log_unknown = tobool(stn_dict.get('log_unknown_sensors', False))
        self._log_unmapped = tobool(stn_dict.get('log_unmapped_sensors', False))
        self._log_packets = tobool(stn_dict.get('log_packets', True))

        loginf("connecting to %s:%s topic=%s unit_system=%s"
               % (self._host, self._port, self._topic, self._unit_system))
        loginf("sensor_map = %s" % self._sensor_map)
        loginf("deltas = %s" % self._deltas)

        self._queue = queue.Queue()
        self._client = mqtt.Client(client_id=self._client_id)
        if self._username:
            self._client.username_pw_set(self._username, self._password)
        if self._tls:
            self._client.tls_set()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect
        self._client.reconnect_delay_set(min_delay=1, max_delay=120)
        try:
            self._client.connect(self._host, self._port, keepalive=60)
        except (OSError, ValueError) as e:
            raise weewx.WeeWxIOError(
                "mqtt connect to %s:%s failed: %s"
                % (self._host, self._port, e))
        self._client.loop_start()

    def closePort(self):
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as e:
            logerr("error during shutdown: %s" % e)

    @property
    def hardware_name(self):
        return self._model

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            loginf("connected to %s:%s" % (self._host, self._port))
            client.subscribe(self._topic)
            loginf("subscribed to %s" % self._topic)
        else:
            logerr("mqtt connect returned rc=%s" % rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            loginf("unexpected disconnect rc=%s; paho will reconnect" % rc)

    def _on_message(self, client, userdata, msg):
        # keep the broker thread fast - parse on the genLoopPackets thread
        self._queue.put(msg.payload)

    def genLoopPackets(self):
        while True:
            try:
                payload = self._queue.get(timeout=60)
            except queue.Empty:
                continue
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8', errors='replace')
            packet = _parse_event(payload, self._unit_system)
            if packet is None:
                if self._log_unknown:
                    loginf("unparsed: %s" % payload)
                continue
            mapped = self.map_to_fields(packet, self._sensor_map)
            if not mapped:
                if self._log_unmapped:
                    loginf("unmapped: %s" % packet)
                continue
            self._calculate_deltas(mapped)
            if self._log_packets:
                logdbg("packet=%s" % mapped)
            yield mapped

    def _calculate_deltas(self, pkt):
        for k, label in self._deltas.items():
            if label in pkt:
                pkt[k] = self._calculate_delta(
                    label, pkt[label], self._counter_values.get(label))
                self._counter_values[label] = pkt[label]

    @staticmethod
    def _calculate_delta(label, newtotal, oldtotal):
        if newtotal is None or oldtotal is None:
            return None
        if newtotal < oldtotal:
            loginf("%s decrement ignored: new=%s old=%s"
                   % (label, newtotal, oldtotal))
            return None
        return newtotal - oldtotal

    @staticmethod
    def map_to_fields(pkt, sensor_map):
        out = {}
        for n, pattern in sensor_map.items():
            label = _find_match(pattern, pkt.keys())
            if label:
                out[n] = pkt.get(label)
        if out:
            for k in ('dateTime', 'usUnits'):
                out[k] = pkt[k]
        return out


# standalone test mode: subscribe to a live broker and exercise the full
# parse -> map -> delta pipeline, printing each step.  works without weewx
# installed (the import block above provides minimal stubs).  flags
# override anything pulled from --config.
def _load_config_section(path, section=DRIVER_NAME):
    try:
        import configobj
    except ImportError:
        raise SystemExit(
            "configobj is required to read weewx.conf: pip install configobj")
    cfg = configobj.ConfigObj(path, file_error=True)
    if section not in cfg:
        raise SystemExit("section [%s] not found in %s" % (section, path))
    return cfg[section]


if __name__ == '__main__':
    import optparse
    import sys
    usage = """%prog [--config FILE] [--host HOST] [--port PORT] [--topic TOPIC]
              [--username USER] [--password PASS] [--tls]
              [--unit-system US|METRIC] [--no-map] [--version]

Test mode connects to a live MQTT broker and prints, for each event:
  raw    : the JSON payload as published
  parsed : the dict the parser produces (fields tagged <field>.<id>.<model>)
  mapped : the dict after sensor_map + deltas (only if a sensor_map is loaded)

With --config, sensor_map / deltas / unit_system / host / port / topic /
username / password / tls are all pulled from the [RTL433MQTT] section of
the file (a real weewx.conf works).  Any explicit flag overrides config."""
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('--config', help='weewx.conf-style file with [%s] section'
                      % DRIVER_NAME)
    parser.add_option('--host')
    parser.add_option('--port', type='int')
    parser.add_option('--topic')
    parser.add_option('--username')
    parser.add_option('--password')
    parser.add_option('--tls', action='store_true', default=None)
    parser.add_option('--unit-system', help='US or METRIC')
    parser.add_option('--no-map', action='store_true', default=False,
                      help='skip sensor_map even if --config provides one')
    parser.add_option('--version', action='store_true', default=False)
    opts, _args = parser.parse_args()

    if opts.version:
        print("rtl433mqtt %s" % DRIVER_VERSION)
        raise SystemExit(0)

    # start with built-in defaults, then layer config, then layer flags
    cfg = {
        'host': DEFAULT_HOST, 'port': DEFAULT_PORT, 'topic': DEFAULT_TOPIC,
        'unit_system': DEFAULT_UNIT_SYSTEM,
        'username': None, 'password': None, 'tls': False,
        'sensor_map': {}, 'deltas': dict(RTL433MQTTDriver.DEFAULT_DELTAS),
    }
    if opts.config:
        sec = _load_config_section(opts.config)
        for k in ('host', 'port', 'topic', 'unit_system',
                  'username', 'password', 'tls'):
            if k in sec:
                cfg[k] = sec[k]
        if 'sensor_map' in sec:
            cfg['sensor_map'] = dict(sec['sensor_map'])
        if 'deltas' in sec:
            cfg['deltas'] = dict(sec['deltas'])

    if opts.host is not None:        cfg['host'] = opts.host
    if opts.port is not None:        cfg['port'] = opts.port
    if opts.topic is not None:       cfg['topic'] = opts.topic
    if opts.username is not None:    cfg['username'] = opts.username
    if opts.password is not None:    cfg['password'] = opts.password
    if opts.tls is not None:         cfg['tls'] = opts.tls
    if opts.unit_system is not None: cfg['unit_system'] = opts.unit_system
    if opts.no_map:                  cfg['sensor_map'] = {}

    cfg['port'] = int(cfg['port'])
    cfg['tls'] = tobool(cfg['tls'])
    cfg['unit_system'] = str(cfg['unit_system']).upper()
    if cfg['unit_system'] not in ('US', 'METRIC'):
        print("warning: unknown unit_system '%s', using METRIC"
              % cfg['unit_system'])
        cfg['unit_system'] = 'METRIC'

    q = queue.Queue()
    def _on_msg(c, u, msg):
        q.put(msg.payload)

    client = mqtt.Client()
    if cfg['username']:
        client.username_pw_set(cfg['username'], cfg['password'])
    if cfg['tls']:
        client.tls_set()
    client.on_message = _on_msg
    try:
        client.connect(cfg['host'], cfg['port'], keepalive=60)
    except (OSError, ValueError) as e:
        raise SystemExit("connect to %s:%s failed: %s" %
                         (cfg['host'], cfg['port'], e))
    client.subscribe(cfg['topic'])
    client.loop_start()

    print("subscribed to %s on %s:%s (unit_system=%s)" %
          (cfg['topic'], cfg['host'], cfg['port'], cfg['unit_system']))
    print("sensor_map entries: %d" % len(cfg['sensor_map']))
    print("deltas: %s" % cfg['deltas'])
    print("waiting for events (Ctrl-C to stop)...")

    counter_values = {}
    n = 0
    try:
        while True:
            try:
                payload = q.get(timeout=5)
            except queue.Empty:
                continue
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8', errors='replace')
            n += 1
            print()
            print("=== event %d ===" % n)
            print("raw    : %s" % payload.strip())
            packet = _parse_event(payload, cfg['unit_system'])
            if packet is None:
                print("parsed : <unparseable or no model>")
                continue
            print("parsed : %s" % packet)
            if cfg['sensor_map']:
                mapped = RTL433MQTTDriver.map_to_fields(
                    packet, cfg['sensor_map'])
                if not mapped:
                    print("mapped : <no sensor_map match>")
                    continue
                # apply deltas just like the live driver
                for dk, label in cfg['deltas'].items():
                    if label in mapped:
                        mapped[dk] = RTL433MQTTDriver._calculate_delta(
                            label, mapped[label], counter_values.get(label))
                        counter_values[label] = mapped[label]
                print("mapped : %s" % mapped)
    except KeyboardInterrupt:
        print()
        print("stopped after %d event(s)" % n)
    finally:
        client.loop_stop()
        client.disconnect()
