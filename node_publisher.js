// node_publisher.js
const mqtt = require('mqtt');
const { v4: uuidv4 } = require('uuid');

const HOST = process.env.MQTT_HOST || '127.0.0.1';
const PORT = process.env.MQTT_PORT || 1884;
const site = 'siteA', room = 'A101', deviceId = 'node-js-pub';
const topic = `devices/${site}/${room}/${deviceId}/telemetry`;

const c = mqtt.connect(`mqtt://${HOST}:${PORT}`);
c.on('connect', () => {
  console.log('Node.js publisher connected');
  setInterval(() => {
    const doc = {
      trace_id: uuidv4(),
      site, room, deviceId,
      lux: 100 + Math.floor(Math.random() * 20),
      occupancy_prob: Math.random(),                 // 0~1
      occupancy: Math.random() > 0.4,
      light_state: 1,
      ts: Date.now(),
      source: 'nodejs_publisher'
    };
    c.publish(topic, JSON.stringify(doc), { qos: 0 });
    console.log('→ published', topic);
  }, 1000);
});
