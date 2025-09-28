# -*- coding: utf-8 -*-
"""
A102 摄像头发布（稳定版）
- 使用 CAP_DSHOW（Windows 更稳）
- 缩小缓冲：CAP_PROP_BUFFERSIZE=1
- 每轮先 grab 多帧再 retrieve，追到最新帧
- 检测重复帧：长时间不变则自动重开摄像头
- 可开 verbose 打印 brightness/diff 便于调试
"""

import cv2, time, json, uuid, argparse
from paho.mqtt import client as mqtt

HOST="127.0.0.1"
PORT=1884
SITE="siteA"
ROOM="A102"
DEV ="cam_dev_A102"
TOPIC=f"devices/{SITE}/{ROOM}/{DEV}/telemetry"

# 阈值/参数（和你现有规则保持一致）
EMA_ALPHA=0.2           # 亮度平滑
ON_PROB=0.85            # ≥ 0.85 视为有人
OFF_SEC=20              # 连续无人 20s 关灯
DIFF_SCALE=20.0         # 帧差缩放，用于合成占用概率
BRIGHT_SCALE=128.0      # 亮度缩放，用于合成占用概率

# —— 可选：打开调试输出（每 N 帧打印一次）——
VERBOSE = False
PRINT_EVERY = 5

def open_camera(index_or_url):
    """统一打开源：整数当索引，字符串当 URL。"""
    if isinstance(index_or_url, str) and (index_or_url.startswith("http://") or index_or_url.startswith("rtsp://")):
        cap = cv2.VideoCapture(index_or_url)
    else:
        cap = cv2.VideoCapture(int(index_or_url), cv2.CAP_DSHOW)  # Windows 下首选 DirectShow
        # 缩小缓冲，尽量拿到最新帧
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    if not cap.isOpened():
        raise RuntimeError(f"Camera/Stream not available: {index_or_url}")
    return cap

def compute_prob(gray, last_gray, ema_bright):
    bright = float(gray.mean())
    ema = bright if ema_bright is None else (EMA_ALPHA*bright + (1-EMA_ALPHA)*ema_bright)
    diff_mean = 0.0
    if last_gray is not None:
        diff_mean = float(cv2.absdiff(gray, last_gray).mean())
    # 合成占用概率：亮度 + 帧差（可按需调整权重）
    occ_prob = min(1.0, max(0.0, (ema/BRIGHT_SCALE)*0.6 + (diff_mean/DIFF_SCALE)*0.6))
    return ema, diff_mean, occ_prob

def main():
    # 如果你用 DroidCam 当 A102，通常索引为 1；也可以改成 URL（如 http://.../video）
    CAM_SRC = 1  # <—— 如需改 URL： CAM_SRC = "http://<ip>:<port>/video"

    cap = open_camera(CAM_SRC)

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.connect(HOST, PORT, 60)
    c.loop_start()

    last_gray = None
    ema_bright = None
    last_occ_ms = 0
    light_state = 0
    stuck_count = 0
    frame_idx = 0

    try:
        while True:
            # 刷新到最新帧：先丢弃几帧（避免消费旧缓存）
            for _ in range(3):
                cap.grab()
            ok, frame = cap.retrieve()
            if not ok:
                time.sleep(0.05)
                continue

            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # 计算亮度/帧差/占用概率
            ema_bright, diff_mean, occ_prob = compute_prob(gray, last_gray, ema_bright)
            occupied = (occ_prob >= ON_PROB)
            last_gray = gray

            # 卡帧/恒定帧检测：长时间 diff≈0 认为源停住，自动重开
            if diff_mean < 0.01:
                stuck_count += 1
            else:
                stuck_count = 0
            if stuck_count >= 50:  # 约 50 帧（~50秒）无变化则重开
                try:
                    cap.release()
                except Exception:
                    pass
                time.sleep(0.2)
                cap = open_camera(CAM_SRC)
                stuck_count = 0
                # 重新打开后继续下轮

            now_ms = int(time.time()*1000)
            if occupied:
                last_occ_ms = now_ms
                light_state = 1
            else:
                if now_ms - last_occ_ms >= OFF_SEC*1000:
                    light_state = 0

            payload = {
                "trace_id": str(uuid.uuid4()),
                "site": SITE, "room": ROOM, "deviceId": DEV,
                "brightness": round(ema_bright, 2),
                "occupancy_prob": round(occ_prob, 3),
                "occupancy": bool(occupied),
                "light_state": int(light_state),
                "ts": now_ms,
                "source": "local"
            }
            c.publish(TOPIC, json.dumps(payload), qos=0)

            if VERBOSE and (frame_idx % PRINT_EVERY == 0):
                print(f"[A102] mean={payload['brightness']:.2f} diff={diff_mean:.2f} "
                      f"occ={payload['occupancy_prob']:.3f} light={light_state}")

            frame_idx += 1
            time.sleep(1.0)

    finally:
        c.loop_stop()
        c.disconnect()
        try:
            cap.release()
        except Exception:
            pass

if __name__ == "__main__":
    main()
