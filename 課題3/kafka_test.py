from kafka import KafkaConsumer

# 確認したい全トピック一覧
TOPICS = [
    "i483-sensors-s2410040-SCD41-co2",
    "i483-sensors-s2410040-SCD41-temperature",
    "i483-sensors-s2410040-SCD41-humidity",
    "i483-sensors-s2410040-BH1750-illumination",
    "i483-sensors-s2410040-DPS310-air_pressure",
    "i483-sensors-s2410040-DPS310-temperature",
    "i483-sensors-s2410040-RPR0521-infrared_illumination",
    "i483-sensors-s2410040-RPR0521-illumination"
]

KAFKA_BOOTSTRAP_SERVERS = "150.65.230.59:9092"

# KafkaConsumer 作成
consumer = KafkaConsumer(
    *TOPICS,  # ← 複数トピック指定
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',  # 過去データも読む
    enable_auto_commit=True,
    group_id='test-consumer-group-all-sensors',
    value_deserializer=lambda x: x.decode('utf-8')
)

print(f"Subscribed to topics:")
for topic in TOPICS:
    print(f" - {topic}")
print("Waiting for messages...")

# メッセージ受信ループ
for message in consumer:
    print(f"[{message.topic}] {message.value}")
