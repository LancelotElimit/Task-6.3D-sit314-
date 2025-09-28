# cam_publisher.py
import cv2, time, json, uuid
from paho.mqtt import client as mqtt
import numpy as np

HOST="127.0.0.1"; PORT=1884
SITE="siteA"; ROOM="A101"; DEV="cam_dev_A101"
TOPIC=f"devices/{SITE}/{ROOM}/{DEV}/telemetry"

# 阈值（可调）
BRIGHT_MIN=30           # 平均灰度阈（0~255）
DIFF_TH=8.0             # 帧差均值阈（像素变化）
EMA_ALPHA=0.2           # 指数滑动平均，用于稳定估计
ON_PROB=0.85            # 占用概率阈值
NO_OCCUPY_OFF_SEC=20    # 20s 无人 → off

cap=cv2.VideoCapture(0)  # 笔记本摄像头
if not cap.isOpened():
    raise RuntimeError("Camera not available")

last_gray=None
ema_bright=None
last_occupied_ts=0
light_state=0           # 初始关灯

c=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.connect(HOST, PORT, 60)
c.loop_start()

try:
    while True:
        ok, frame=cap.read()
        if not ok: time.sleep(0.1); continue
        gray=cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        bright=float(gray.mean())                 # 平均亮度≈“lux”
        ema_bright=bright if ema_bright is None else (EMA_ALPHA*bright+(1-EMA_ALPHA)*ema_bright)

        diff_score=0.0
        if last_gray is not None:
            diff=cv2.absdiff(gray, last_gray)
            diff_score=float(diff.mean())
        last_gray=gray

        # 简单概率：亮 + 有帧差 → 高占用；否则衰减
        occ_prob=float(min(1.0, max(0.0, (ema_bright/128.0)*0.6 + (diff_score/20.0)*0.6)))
        occupied = occ_prob >= ON_PROB
        now_ms=int(time.time()*1000)

        if occupied:
            last_occupied_ts=now_ms
            light_state=1
        else:
            if (now_ms-last_occupied_ts) >= NO_OCCUPY_OFF_SEC*1000:
                light_state=0

        payload={
            "trace_id": str(uuid.uuid4()),
            "site": SITE, "room": ROOM, "deviceId": DEV,
            "brightness": round(ema_bright,2),
            "occupancy_prob": round(occ_prob,3),
            "occupancy": bool(occupied),
            "light_state": int(light_state),
            "ts": now_ms
        }
        c.publish(TOPIC, json.dumps(payload), qos=0)
        time.sleep(1.0)
finally:
    c.loop_stop(); c.disconnect(); cap.release()
