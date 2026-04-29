# installer for the weewx-rtl433-mqtt driver
# Distributed under the terms of the GNU Public License (GPLv3)

from weecfg.extension import ExtensionInstaller

def loader():
    return RTL433MQTTInstaller()

class RTL433MQTTInstaller(ExtensionInstaller):
    def __init__(self):
        super(RTL433MQTTInstaller, self).__init__(
            version="0.1",
            name='rtl433mqtt',
            description='Capture rtl_433 events from an MQTT broker',
            author="",
            author_email="",
            files=[('bin/user', ['bin/user/rtl433mqtt.py'])]
            )
