from kafka import KafkaProducer
import paho.mqtt.client as mqtt
from collections import deque
from datetime import timedelta
import time, logging, re

# ──★ 固有設定 ───────────────────────────────────────
STUDENT_ID        = "s2410040"
MQTT_HOST, MQTT_PORT = "150.65.230.59", 1883
KAFKA_BOOTSTRAP   = "150.65.230.59:9092"

# ① センサ入力（MQTT）
MQTT_BH1750 = f"i483/sensors/{STUDENT_ID}/BH1750/illumination"
MQTT_CO2    = f"i483/sensors/{STUDENT_ID}/SCD41/co2"

# ② 処理結果（Kafka）
AVG_TOPIC      = f"i483-sensors-{STUDENT_ID}-BH1750_avg_illumination"
CO2_FLAG_TOPIC = f"i483-sensors-{STUDENT_ID}-co2_threshold_crossed"

# ③ LED 制御（MQTT へ yes/no を出す）
LED_CO2_TOPIC  = f"i483/actuators/{STUDENT_ID}/led-co2"
# ────────────────────────────────────────────────
WINDOW         = timedelta(minutes=5)
AVG_INTERVAL   = 30           # [s]
CO2_THRESHOLD  = 700.0        # [ppm]
# ────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ---------- Kafka Producer ---------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda s: s.encode("utf-8"),
)

# ---------- MQTT Client ------------------------------------------------------
mqtt_cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# ---------- 内部状態 ----------------------------------------------------------
illum_buf = deque()      # (timestamp, value)
co2_is_high = None       # None: 未受信, True/False: 受信済み
num_re = re.compile(r"[-+]?\d*\.\d+|\d+")

# ---------- MQTT コールバック -------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    logging.info("MQTT connected: %s", mqtt.connack_string(rc))
    client.subscribe([(MQTT_BH1750, 0), (MQTT_CO2, 0)])

def on_message(client, userdata, msg):
    global co2_is_high
    m = num_re.search(msg.payload.decode())
    if not m:
        return
    val = float(m.group())

    if msg.topic == MQTT_BH1750:
        illum_buf.append((time.time(), val))

    elif msg.topic == MQTT_CO2:
        co2_is_high = val > CO2_THRESHOLD   # 状態のみ更新

mqtt_cli.on_connect, mqtt_cli.on_message = on_connect, on_message
mqtt_cli.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
mqtt_cli.loop_start()

# ---------- メインループ：30 秒ごと実行 --------------------------------------
next_tick = time.time() + AVG_INTERVAL
try:
    while True:
        now = time.time()
        if now >= next_tick:
            next_tick += AVG_INTERVAL

            # --- ① 照度 5 分平均 --------------------------------------------
            cutoff = now - WINDOW.total_seconds()
            while illum_buf and illum_buf[0][0] < cutoff:
                illum_buf.popleft()

            if illum_buf:
                avg = sum(v for _, v in illum_buf) / len(illum_buf)
                producer.send(AVG_TOPIC, f"{avg:.2f}")
                logging.info("Avg illum (5 min): %.2f lux", avg)
            else:
                logging.info("No illum data in last 5 min – skip")

            # --- ② CO₂ フラグ ＋ LED 制御 -----------------------------------
            if co2_is_high is not None:
                flag = "yes" if co2_is_high else "no"
                # Kafka へ
                producer.send(CO2_FLAG_TOPIC, flag)
                # MQTT へ LED 指令
                mqtt_cli.publish(LED_CO2_TOPIC, flag, qos=0)
                logging.info("CO₂ flag periodic → %s  (sent to %s)",
                             flag, LED_CO2_TOPIC)

        producer.flush(0.1)   # Kafka 送信バッファ
        time.sleep(0.2)       # ループ負荷抑制
except KeyboardInterrupt:
    logging.info("Interrupted – shutdown.")
finally:
    mqtt_cli.loop_stop()
    mqtt_cli.disconnect()
    producer.close()
