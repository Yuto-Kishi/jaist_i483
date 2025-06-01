#(b) MQTTを利用して、(a)で公開されているデータを受信するプログラムを開発せよ。
import paho.mqtt.client as mqtt

# MQTT設定
STUDENT_ID = "s2410040"
MQTT_HOST = "150.65.230.59"
MQTT_PORT = 1883
TOPIC = f"i483/sensors/{STUDENT_ID}/#"

# 接続時のコールバック
def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe(TOPIC)
    print(f"Subscribed to: {TOPIC}")

# メッセージ受信時のコールバック
def on_message(client, userdata, msg):
    print(f"[{msg.topic}] {msg.payload.decode()}")

# MQTTクライアント初期化
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# 接続・受信開始
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_forever()
