import json
from paho.mqtt import client as mqtt

HOST = "broker.hivemq.com"
PORT = 1883
topic = "devices/siteA/room101/dev123/telemetry"
payload = {
    "site":"siteA","room":"room101","deviceId":"dev123",
    "lux":80,"temp_center_c":24.2,"occupancy_prob":0.1,
    "occupancy": False,"light_state": 1
}
def on_connect(c, u, f, rc, props=None):
    c.publish(topic, json.dumps(payload), qos=0)
    c.disconnect()

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.on_connect = on_connect
c.connect(HOST, PORT, 60)
c.loop_forever()
