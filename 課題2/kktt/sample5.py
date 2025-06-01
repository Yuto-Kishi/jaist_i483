from kafka import KafkaProducer, KafkaConsumer
from collections import deque
import paho.mqtt.client as mqtt
from datetime import timedelta
import threading
import time
import logging

# ──★ 固有設定 ───────────────────────────────────────
STUDENT_ID        = "s2410040" # こちらは実際のIDに合わせてください
MQTT_HOST, MQTT_PORT = "150.65.230.59", 1883
KAFKA_BOOTSTRAP   = "150.65.230.59:9092"

# ① センサ入力（Kafka）
KAFKA_BH1750      = f"i483-sensors-{STUDENT_ID}-BH1750-illumination"
KAFKA_CO2         = f"i483-sensors-{STUDENT_ID}-SCD41-co2"
KAFKA_LED_CTRL    = f"i483-actuators-{STUDENT_ID}" # このトピックはESP32のGPIO2制御とは別系統

# ② 処理結果（Kafka）
AVG_TOPIC         = f"i483-sensors-{STUDENT_ID}-BH1750_avg_illumination"
CO2_FLAG_TOPIC    = f"i483-sensors-{STUDENT_ID}-co2_threshold_crossed" # KafkaへのCO2状態記録用

# ③ LED 制御（MQTT へ yes/no を出す）
LED_CO2_TOPIC     = f"i483/actuators/{STUDENT_ID}/led-co2" # ESP32が購読するCO2状態トピック

WINDOW            = timedelta(minutes=5)
AVG_INTERVAL      = 30  # [s]
CO2_THRESHOLD     = 700.0  # [ppm]

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

# LED 点滅状態管理 (KAFKA_LED_CTRL トピックからの指示に基づくローカルの点滅シミュレーション用)
led_state = threading.Event()

def blink_led(): # この関数は KAFKA_LED_CTRL トピックにより制御されるもので、ESP32のGPIO2とは直接関係ありません
    while True:
        if led_state.is_set():
            # logging.debug("[Simulated LED] ON")
            time.sleep(0.5)
            # logging.debug("[Simulated LED] OFF")
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
    group_id=f"{STUDENT_ID}-processor-display-control", # 必要に応じてgroup_idを調整
    value_deserializer=lambda b: b.decode("utf-8")
)

# 状態保持
illum_buf = deque()
co2_latest = None # 最新のCO2値（ppm）。AVG_INTERVALごとのKafka送信に使用。
last_avg_time = time.time()

# Kafka Loop -------------------------------------------------------
print("Start Kafka processing loop...")
print("--------------------------------------------------")
try:
    for msg in kafka_consumer:
        topic = msg.topic
        payload = msg.value.strip()
        now = time.time()

        if topic == KAFKA_BH1750:
            try:
                value = float(payload)
                illum_buf.append((now, value))
            except ValueError:
                logging.warning(f"Invalid float from {topic}: {payload}")
                continue

            if now - last_avg_time >= AVG_INTERVAL:
                cutoff = now - WINDOW.total_seconds()
                while illum_buf and illum_buf[0][0] < cutoff:
                    illum_buf.popleft()

                if illum_buf:
                    avg = sum(v for _, v in illum_buf) / len(illum_buf)
                    logging.info("[AVG] %.2f lux", avg)
                    print(f"** 平均照度: {avg:.2f} lux **")
                    producer.send(AVG_TOPIC, f"{avg:.2f}")
                else:
                    logging.info("No illumination data in last 5 minutes")
                    print("** 平均照度: データなし (過去5分間) **")

                # CO2 に基づくフラグをKafkaに定期的（AVG_INTERVALごと）に出力
                if co2_latest is not None:
                    flag_for_kafka = "yes" if co2_latest > CO2_THRESHOLD else "no"
                    logging.info("[THRESHOLD] CO2 based flag (periodic for Kafka topic %s) = %.2f ppm -> %s",
                                 CO2_FLAG_TOPIC, co2_latest, flag_for_kafka)
                    producer.send(CO2_FLAG_TOPIC, flag_for_kafka)
                
                last_avg_time = now
                print("--------------------------------------------------")


        elif topic == KAFKA_CO2:
            try:
                co2_value = float(payload)
                co2_latest = co2_value # 最新のCO2値を更新 (AVG_INTERVALごとのKafka送信で使用)
                
                # ESP32のLED制御のための状態判断とMQTT送信
                current_co2_status_for_led = "yes" if co2_value >= CO2_THRESHOLD else "no"
                print(f"CO2濃度: {co2_value:.2f} ppm ({current_co2_status_for_led})")
                
                logging.info(f"CO2 updated: {co2_value:.2f} ppm. Status for ESP32 LED: {current_co2_status_for_led}. Publishing to MQTT topic: {LED_CO2_TOPIC}")
                mqtt_cli.publish(LED_CO2_TOPIC, current_co2_status_for_led, qos=0, retain=False)

            except ValueError:
                logging.warning(f"Invalid CO2 value: {payload}")

        elif topic == KAFKA_LED_CTRL: # このトピックはESP32のGPIO2制御とは別系統の指示
            logging.info(f"[ACTUATOR] Received command on {KAFKA_LED_CTRL}: {payload}")
            if payload.lower() == "yes":
                led_state.set() # ローカルのblink_ledスレッドの制御
                logging.info("Simulated LED blinking activated via Kafka.")
            else:
                led_state.clear() # ローカルのblink_ledスレッドの制御
                logging.info("Simulated LED blinking deactivated via Kafka.")

except KeyboardInterrupt:
    logging.info("Interrupted by user. Shutting down...")
finally:
    logging.info("Stopping Kafka consumer...")
    if 'kafka_consumer' in locals() and kafka_consumer:
      kafka_consumer.close()
    logging.info("Stopping MQTT client...")
    if 'mqtt_cli' in locals() and mqtt_cli:
      mqtt_cli.loop_stop()
      mqtt_cli.disconnect()
    logging.info("Closing Kafka producer...")
    if 'producer' in locals() and producer:
      producer.close()
    print("Cleanup complete. Exiting.")
