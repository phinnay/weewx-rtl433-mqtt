# weewx-rtl433-mqtt

A WeeWX driver that consumes rtl_433 events from an MQTT broker. Sibling
to (and inspired by) [weewx-sdr](https://github.com/matthewwall/weewx-sdr),
which spawns rtl_433 as a subprocess on the WeeWX host. This driver
expects rtl_433 to be running elsewhere and publishing JSON events via
`rtl_433 -F mqtt://broker:1883,events`.

## Layout

```
bin/user/rtl433mqtt.py   single-file driver. module name `rtl433mqtt`,
                         driver = user.rtl433mqtt, config [RTL433MQTT]
install.py               WeeWX ExtensionInstaller
README.md                user-facing docs (markdown)
changelog                per-version notes
license                  GPLv3
```

User-facing docs live in `README.md`. This file is for design context
that isn't obvious from the code.

## Design decisions worth knowing

- **No per-sensor parsers.** weewx-sdr has ~80 `*Packet` classes that
  normalize field names and units; this driver doesn't — rtl_433 has
  already decoded the radio. Fields pass through verbatim, tagged as
  `<rtl_433_field>.<sensor_id>.<rtl_433_model>`.
- **Sensor map syntax is intentionally NOT compatible with weewx-sdr's**
  `<obs>.<id>.<PacketClassName>` format. The model part is rtl_433's
  literal `model` field (`Acurite-Tower` with hyphen, not
  `AcuriteTowerPacket`). Maps cannot be copy-pasted between drivers.
- **Generic suffix-based unit conversion** driven by a `unit_system`
  config option. Conversion table covers `_C/_F`, `_mm/_in`,
  `_km_h/_mi_h`, `_kph/_mph`, `_m_s` (Fineoffset-WH24 and friends),
  `_hPa/_inHg`. Anything else passes through unchanged.
- **Channel encoded into sensor_id as `<id>:<channel>`** when the rtl_433
  event has a `channel` field (Acurite multi-channel, AmbientWeather-
  WH31E, Hideki TS04). Channel is hardware-set and stable across
  battery changes; id rotates on power-cycle. Maps use
  `*:N.<model>` to pin a channel without chasing the id.
- **Battery normalization** to WeeWX convention (0=ok, 1=low) handles
  `battery_ok` / `battery_low` / `battery="OK"` variants.
- **Default-true `ignore_lwt`** silently drops MQTT LWT / birth / status
  messages (anything not a JSON object). `--show-status` opts back in
  for test-mode visibility. In driver mode, off-flag falls through to
  the existing `log_unknown_sensors` path.

## Test mode (standalone)

See `README.md` for usage examples. Internals: `python3 bin/user/rtl433mqtt.py [flags]` runs the full parse → map →
delta pipeline against a live broker without needing WeeWX installed.
The module's `import weewx` block falls back to stubs (`weewx.US`,
`weewx.METRIC`, `AbstractDevice`, `AbstractConfEditor`, `tobool`) when
WeeWX is absent. `--config FILE` pulls everything from a `[RTL433MQTT]`
section via `configobj`. `paho-mqtt` is the only hard runtime dep.

## paho-mqtt version handling

Detects paho 2.x via `hasattr(mqtt, 'CallbackAPIVersion')`. When
present, constructs `Client` with `callback_api_version=VERSION2` and
uses v2 callback signatures (`on_connect(client, userdata,
connect_flags, reason_code, properties)`, similar for `on_disconnect`).
Falls back to v1 on paho 1.x. Both `_on_connect` and `_on_disconnect`
accept `*args` and dispatch on arity so the same method serves both
lines. `_rc_to_int()` normalizes paho 2.x's `ReasonCode` objects to
plain ints.

Important context: paho 2.x deprecates the v1 callback API itself, not
just the unspecified default. Pinning `VERSION1` does NOT silence the
warning — only `VERSION2` does, and it requires the new callback
signatures. (Earlier commit `9f486b2` got this wrong; `55fcd52` is the
real fix.)

## Common gotchas (for future debugging)

- "no sensor_map match" → almost always a `unit_system` mismatch with
  the field suffix in the map. METRIC ⇒ `_C` / `_mm` / `_km_h`,
  US ⇒ `_F` / `_in` / `_mi_h`. Use test mode to see what suffix the
  parser actually produces, then write the map to match.
- Sensor IDs rotate on battery / power cycle (Fineoffset, AmbientWeather,
  several Acurite). Use `*` in the id slot; pin the model. For multi-
  channel sensors, use `*:N`.
- Cumulative counters (`rain_in`, `rain_mm`, `strike_count`) must be
  mapped to `rain_total` / `strike_count` first, then converted via
  `[[deltas]]`. Mapping them directly to `rain` /
  `lightning_strike_count` yields cumulative numbers, which is wrong.
- First event after weewx restart has `delta = None` (no prior
  counter). Expected; weewx handles `None` gracefully.
- Topic structure depends on the rtl_433 invocation. Default
  `rtl_433/<host>/events` matches `rtl_433/+/events`. Some setups use
  `rtl_433/<model>/<id>` (custom format string), which needs
  `rtl_433/+/+`.

## Remote

`origin` at `https://gitea.home.catfeesh.com/phinnay/weewx-rtl433-mqtt.git`,
branch `main`. The gitea repo was originally named `weewx-sdr-mqtt` and
renamed on 2026-04-30 to match the local folder.
