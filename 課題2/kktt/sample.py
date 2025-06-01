from kafka import KafkaProducer
import paho.mqtt.client as mqtt

STUDENT_ID = "s2410040"
MQTT_HOST = "150.65.230.59"
MQTT_PORT = 1883
KAFKA_BOOTSTRAP = "150.65.230.59:9092"

MQTT_TOPICS = [
    f"i483/sensors/{STUDENT_ID}/BH1750/illumination",
    f"i483/sensors/{STUDENT_ID}/SCD41/co2",
    # 必要に応じて他のセンサトピックも追加
]

KAFKA_MAP = {
    f"i483/sensors/{STUDENT_ID}/BH1750/illumination": f"i483-sensors-{STUDENT_ID}-BH1750-illumination",
    f"i483/sensors/{STUDENT_ID}/SCD41/co2": f"i483-sensors-{STUDENT_ID}-SCD41-co2"
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda s: s.encode('utf-8')
)

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to MQTT broker with result code", rc)
    for topic in MQTT_TOPICS:
        client.subscribe(topic)

def on_message(client, userdata, msg):
    print(f"[MQTT→Kafka] {msg.topic} → {msg.payload.decode()}")
    if msg.topic in KAFKA_MAP:
        kafka_topic = KAFKA_MAP[msg.topic]
        producer.send(kafka_topic, msg.payload.decode())

mqtt_cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_cli.on_connect = on_connect
mqtt_cli.on_message = on_message

mqtt_cli.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
mqtt_cli.loop_forever()

