// index.js
const { MongoClient } = require('mongodb');

let client; // 连接重用以提升并发下性能

exports.handler = async (event) => {
  const uri = process.env.MONGODB_URI; // 和 Python 版同一个连接串
  const dbName = process.env.ATLAS_DB || 'iot';
  const eventsCol = process.env.ATLAS_EVENTS_COL || 'events';

  if (!client) {
    client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
  }
  await client.connect();
  const db = client.db(dbName);
  const events = db.collection(eventsCol);

  // 同时兼容 Function URL（event.body 为字符串）和 CLI/SDK 触发（event 为对象）
  let doc = {};
  try {
    doc = typeof event?.body === 'string' ? JSON.parse(event.body) : (event || {});
  } catch (_) {
    doc = event || {};
  }

  doc.ts = doc.ts ?? Date.now();
  doc.source = 'lambda_node'; // 区分来源，便于汇报
  // 可选：若你想顺便落 controls：
  // if (typeof doc.occupancy_prob === 'number') { ... 写 iot.controls ... }

  const res = await events.insertOne(doc);

  return {
    statusCode: 200,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ok: true, inserted_id: res.insertedId })
  };
};
