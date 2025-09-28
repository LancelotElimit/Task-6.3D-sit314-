# -*- coding: utf-8 -*-
import os, json, time, threading, queue, uuid
from paho.mqtt import client as mqtt
from pymongo import MongoClient

# --------- 环境变量 / 连接参数 ----------
MONGODB_URI = os.environ.get("MONGODB_URI") or os.environ.get("MONGODBURL") or os.environ.get("MONGODB")
if not MONGODB_URI:
    MONGODB_URI = "mongodb://localhost:27017"

MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1884"))

DB_NAME = os.environ.get("DB_NAME", "iot")
EVENTS_COL = os.environ.get("EVENTS_COL", "events")
DEVICES_COL = os.environ.get("DEVICES_COL", "devices")
CONTROLS_COL = os.environ.get("CONTROLS_COL", "controls")

# 阈值（沿用你现有规则）
ON_PROB = float(os.environ.get("ON_PROB", "0.85"))

# --------- Mongo 连接 ----------
mongo = MongoClient(MONGODB_URI)
db = mongo[DB_NAME]
events = db[EVENTS_COL]
devices = db[DEVICES_COL]
controls = db[CONTROLS_COL]

# 简单的写队列（异步入库，降低延迟抖动）
write_q = queue.Queue(maxsize=10000)
ctrl_q  = queue.Queue(maxsize=10000)

def writer():
    while True:
        typ, payload = write_q.get()
        try:
            if typ == "event":
                doc = payload
                pre = int(time.time()*1000)
                res = events.insert_one(doc)
                post = int(time.time()*1000)
                events.update_one({"_id": res.inserted_id}, {"$set": {"preInsertAt": pre, "postInsertAt": post}})
                # 设备 upsert
                devices.update_one(
                    {"deviceId": doc["deviceId"]},
                    {"$set": {
                        "lastSeen": doc["ts"],
                        "room": doc.get("room"),
                        "site": doc.get("site"),
                        "lastLux": doc.get("lux"),
                        "lastTemp": doc.get("temp_center_c"),
                        "light_state": doc.get("light_state"),
                    }, "$setOnInsert": {"firstSeen": doc["ts"]}},
                    upsert=True
                )
            elif typ == "control":
                controls.insert_one(payload)
        except Exception as e:
            print("writer error:", e)

threading.Thread(target=writer, daemon=True).start()

# 可选：把控制下发也发到 MQTT（开启=1，关闭=0）
ENABLE_CTRL_PUBLISH = True

# 为了在 on_message 内发布控制消息，建一个独立的 publisher client
pub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
pub_client.connect(MQTT_HOST, MQTT_PORT, 60)
pub_client.loop_start()

# --------- MQTT 订阅 ----------
TOPIC = "devices/+/+/+/telemetry"

def on_connect(c, u, f, rc, props=None):
    print(f"Listening mqtt://{MQTT_HOST}:{MQTT_PORT} ...")
    print("MQTT connected:", "Success" if rc == 0 else rc)
    c.subscribe(TOPIC, qos=0)

def parse_payload(raw):
    """
    稳健解析：支持 string/嵌套 string，最终必须是 dict，否则返回 None
    """
    try:
        raw = raw.decode("utf-8").strip()
        doc = json.loads(raw)
        if isinstance(doc, str):
            doc = json.loads(doc)
        if not isinstance(doc, dict):
            return None, "non-object payload"
        return doc, None
    except Exception as e:
        return None, f"parse error: {e}"

def on_message(c, u, msg):
    doc, err = parse_payload(msg.payload)
    if err:
        print("skip:", err)
        return

    for k in ("deviceId", "site", "room"):
        if k not in doc:
            print("skip: missing", k, "payload=", {kk: doc.get(kk) for kk in ("site","room","deviceId")})
            return

    # 时间戳与主题
    now_ms = int(time.time()*1000)
    ts = doc.get("ts")
    if not isinstance(ts, (int, float)):
        ts = now_ms
        doc["ts"] = ts

    doc["ingestedAt"] = now_ms
    doc["topic"] = msg.topic

    # trace_id：保留发布端的；没有则生成一个
    if not doc.get("trace_id"):
        doc["trace_id"] = str(uuid.uuid4())

    # 简单异常标记（保持你之前的一致性）
    light = doc.get("light_state")
    occ = doc.get("occupancy")
    doc["anomaly_flag"] = bool(light == 1 and occ is False)
    doc["anomaly_type"] = "light_on_no_presence" if doc["anomaly_flag"] else None

    # 入队写入 event（异步）
    try:
        write_q.put_nowait(("event", dict(doc)))
    except queue.Full:
        print("WARN: event queue full, drop one")

    # ---------- 端到端 trace：控制指令生成 & 落库 & 可选下发 ----------
    # 1) 计算 desired_light（优先 occupancy_prob；缺失时回退 occupancy）
    occ_prob = doc.get("occupancy_prob")
    if isinstance(occ_prob, (int, float)):
        desired_light = 1 if float(occ_prob) >= ON_PROB else 0
        reason = f"occupancy_prob({occ_prob})>={ON_PROB}" if desired_light else f"occupancy_prob({occ_prob})<{ON_PROB}"
    else:
        # 回退：无概率时用 occupancy 布尔
        desired_light = 1 if bool(doc.get("occupancy")) else 0
        reason = "fallback_by_occupancy_bool"

    ctrl = {
        "trace_id": doc["trace_id"],
        "site": doc["site"],
        "room": doc["room"],
        "deviceId": doc["deviceId"],
        "desired_light": int(desired_light),
        "ts_detect": int(doc["ts"]),           # 检测时刻（事件的 ts）
        "ts_control": now_ms,                  # 产生控制决策的时刻
        "reason": reason,
        "event_topic": doc["topic"],
        "event_id": None                       # 如需可在 writer 里补回 inserted_id（这里先留空）
    }

    # 2) 控制文档入库（异步）
    try:
        ctrl_q.put_nowait(("control", dict(ctrl)))
    except queue.Full:
        print("WARN: control queue full, drop one")
    else:
        # 控制写入交给写线程
        write_q.put(("control", dict(ctrl)))

    # 3) 可选：把控制指令也发布到 MQTT（便于下游执行）
    if ENABLE_CTRL_PUBLISH:
        ctrl_topic = f"controls/{doc['site']}/{doc['room']}/{doc['deviceId']}/set"
        try:
            pub_client.publish(ctrl_topic, json.dumps(ctrl), qos=0)
        except Exception as e:
            print("ctrl publish error:", e)

    # （可选）打印端内观测延迟，方便你现场观测
    latency = now_ms - int(ts)
    if 0 <= latency < 60000:
        print(f"latency(ms) = {latency} topic= {msg.topic}")

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.connect(MQTT_HOST, MQTT_PORT, 60)

try:
    mqttc.loop_forever()
finally:
    pub_client.loop_stop()
    pub_client.disconnect()
