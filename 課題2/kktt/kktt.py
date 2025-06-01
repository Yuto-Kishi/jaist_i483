from kafka import KafkaProducer
from kafka import KafkaConsumer
from collections import deque
import paho.mqtt.client as mqtt
from datetime import timedelta
import threading
import time
import logging
import re

# ──★ 固有設定 ───────────────────────────────────────
STUDENT_ID        = "s2410040"
MQTT_HOST, MQTT_PORT = "150.65.230.59", 1883
KAFKA_BOOTSTRAP   = "150.65.230.59:9092"

# ① センサ入力（Kafka）
KAFKA_BH1750 = f"i483-sensors-{STUDENT_ID}-BH1750-illumination"
KAFKA_CO2    = f"i483-sensors-{STUDENT_ID}-SCD41-co2"
KAFKA_LED_CTRL = f"i483-actuators-{STUDENT_ID}"

# ② 処理結果（Kafka）
AVG_TOPIC      = f"i483-sensors-{STUDENT_ID}-BH1750_avg_illumination"
CO2_FLAG_TOPIC = f"i483-sensors-{STUDENT_ID}-co2_threshold_crossed"

# ③ LED 制御（MQTT へ yes/no を出す）
LED_CO2_TOPIC  = f"i483/actuators/{STUDENT_ID}/led-co2"

WINDOW         = timedelta(minutes=5)
AVG_INTERVAL   = 30           # [s]
CO2_THRESHOLD  = 700.0        # [ppm]

# ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# Kafka Producer ---------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda s: s.encode("utf-8"),
)

# MQTT Client ------------------------------------------------------
mqtt_cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_cli.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
mqtt_cli.loop_start()

# LED 点滅状態管理
led_state = threading.Event()

def blink_led():
    while True:
        if led_state.is_set():
            print("[LED] ON")
            time.sleep(0.5)
            print("[LED] OFF")
            time.sleep(0.5)
        else:
            time.sleep(0.1)

threading.Thread(target=blink_led, daemon=True).start()

# Kafka Consumer ---------------------------------------------------
kafka_consumer = KafkaConsumer(
    KAFKA_BH1750,
    KAFKA_CO2,
    KAFKA_LED_CTRL,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    group_id=f"{STUDENT_ID}-processor"
)

# 状態保持
illum_buf = deque()
co2_latest = None
last_avg_time = time.time()

# Kafka Loop -------------------------------------------------------
print("Start Kafka processing loop...")
for msg in kafka_consumer:
    topic = msg.topic
    payload = msg.value.decode()
    now = time.time()

    if KAFKA_BH1750 in topic:
        value = float(payload)
        illum_buf.append((now, value))

        # 照度平均を30秒ごとに出力
        if now - last_avg_time >= AVG_INTERVAL:
            cutoff = now - WINDOW.total_seconds()
            while illum_buf and illum_buf[0][0] < cutoff:
                illum_buf.popleft()

            if illum_buf:
                avg = sum(v for _, v in illum_buf) / len(illum_buf)
                logging.info("[AVG] %.2f lux", avg)
                producer.send(AVG_TOPIC, f"{avg:.2f}")
            else:
                logging.info("No illumination data in last 5 minutes")

            # CO2フラグ出力とLED指令
            if co2_latest is not None:
                flag = "yes" if co2_latest > CO2_THRESHOLD else "no"
                logging.info("[THRESHOLD] CO2 = %.2f ppm → %s", co2_latest, flag)
                producer.send(CO2_FLAG_TOPIC, flag)
                mqtt_cli.publish(LED_CO2_TOPIC, flag, qos=0)

            last_avg_time = now

    elif KAFKA_CO2 in topic:
        co2_latest = float(payload)

    elif KAFKA_LED_CTRL in topic:
        print(f"[ACTUATOR] {payload}")
        if payload.strip().lower() == "yes":
            led_state.set()
        else:
            led_state.clear()

