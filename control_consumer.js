// control_consumer.js
const mqtt = require('mqtt');

const HOST = process.env.MQTT_HOST || '127.0.0.1';
const PORT = process.env.MQTT_PORT || 1884;

const c = mqtt.connect(`mqtt://${HOST}:${PORT}`);
c.on('connect', () => {
  console.log('Node.js control consumer connected');
  c.subscribe('control/+/+/+/light/set', { qos: 0 });
});

c.on('message', (topic, payload) => {
  try {
    const msg = JSON.parse(payload.toString());
    console.log('← control', topic, msg);
    // 可选：回一条 ack
    // const ackTopic = topic.replace('/set', '/ack');
    // c.publish(ackTopic, JSON.stringify({ ok: true, ts: Date.now() }));
  } catch (e) {
    console.log('control raw', topic, payload.toString());
  }
});
