#!/usr/bin/env python3
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# --- 設定 ---
MQTT_BROKER  = "150.65.230.59"
MQTT_PORT    = 1883
MQTT_TOPIC   = "i483/sensors/s2410040/#"  # 必要に応じて他学籍番号も対象にする

KAFKA_BROKER = "150.65.230.59:9092"
KAFKA_TOPIC  = "i483-allsensors"

# Kafka プロデューサー初期化
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

# MQTT メッセージ受信時のコールバック
def on_message(client, userdata, msg):
    topic = msg.topic.replace("/", "-")        # 例: i483-sensors-s2410224-SCD41-co2
    payload = msg.payload.decode("utf-8")      # 例: 488.0
    timestamp_ms = int(time.time() * 1000)

    print(f"[MQTT] {topic} → {payload}")

    # Kafka にまとめて送るメッセージ構造
    kafka_record = f"{topic},{timestamp_ms},{payload}"
    producer.send(KAFKA_TOPIC, kafka_record.encode("utf-8"))
    producer.flush()

    print(f"[Kafka] {KAFKA_TOPIC} ← {kafka_record}")

# MQTT クライアント初期化
client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT)
client.subscribe(MQTT_TOPIC)

print(f"Waiting for messages on {MQTT_TOPIC} …")
client.loop_forever()