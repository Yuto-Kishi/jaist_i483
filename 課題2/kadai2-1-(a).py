import time, sys
from machine import Pin, I2C
import network
from umqtt.robust import MQTTClient

# ──★ 編集ポイント ─────────────────────────────
STUDENT_ID = "s2410040"
WIFI_SSID  = "JAISTALL"
WIFI_PASS  = ""
MQTT_HOST  = "150.65.230.59"
MQTT_PORT  = 1883
PUB_INTERVAL = 15  # [s]

LED_PIN = 2
BLINK_INTERVAL_MS = 500
LED_TOPIC = f"i483/actuators/{STUDENT_ID}/led-co2"
# ──────────────────────────────────────────────

# ===== 接続 =====
def connect_net() -> MQTTClient:
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    print("MAC:", wlan.config('mac').hex())
    if not wlan.isconnected():
        wlan.connect(WIFI_SSID, WIFI_PASS)
        while not wlan.isconnected():
            time.sleep_ms(200)
    print("Wi-Fi connected:", wlan.ifconfig())

    cli = MQTTClient(client_id=STUDENT_ID.encode(),
                     server=MQTT_HOST, port=MQTT_PORT, keepalive=30)
    cli.set_callback(on_led_msg)
    cli.connect()
    cli.subscribe(LED_TOPIC)
    print("MQTT connected")
    return cli

led = Pin(LED_PIN, Pin.OUT)
blink_mode = False
last_toggle = time.ticks_ms()

def on_led_msg(topic, payload):
    global blink_mode, last_toggle
    if topic == LED_TOPIC.encode():
        if payload == b"yes":
            blink_mode = True
            last_toggle = time.ticks_ms()
            led.value(1)
        else:
            blink_mode = False
            led.value(0)

mqtt = connect_net()

# ===== センサークラス定義 =====

class SCD41:
    def __init__(self, i2c):
        self.i2c = i2c
        self.addr = 0x62

    def crc8(self, data):
        crc = 0xFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 0x80:
                    crc = (crc << 1) ^ 0x31
                else:
                    crc = crc << 1
                crc &= 0xFF
        return crc

    def write_cmd(self, cmd):
        self.i2c.writeto(self.addr, bytes(cmd))

    def stop_measurement(self):
        self.write_cmd([0x3F, 0x86])

    def start_periodic_measurement(self):
        self.write_cmd([0x21, 0xB1])

    def data_ready(self):
        self.write_cmd([0xE4, 0xB8])
        time.sleep_ms(1)
        ready_bytes = self.i2c.readfrom(self.addr, 3)
        return (ready_bytes[1] & 0x07) != 0

    def read_measurement(self):
        self.write_cmd([0xEC, 0x05])
        time.sleep_ms(1)
        data = self.i2c.readfrom(self.addr, 9)
        for i in range(0, 9, 3):
            if self.crc8(data[i:i+2]) != data[i+2]:
                print("CRC error!")
                return None
        co2 = (data[0] << 8) | data[1]
        temp_raw = (data[3] << 8) | data[4]
        hum_raw = (data[6] << 8) | data[7]
        temp = -45 + 175 * (temp_raw / 65535.0)
        hum = 100 * (hum_raw / 65535.0)
        return co2, temp, hum

    def init(self):
        self.stop_measurement()
        time.sleep(1)
        self.start_periodic_measurement()
        time.sleep(5)

class DPS310:
    def __init__(self, i2c):
        self.i2c = i2c
        self.addr = 0x77
        self.i2c.writeto_mem(self.addr, 0x0C, b'\x89')
        time.sleep_ms(40)
        self.coef = self.read_coefficient()
        self.i2c.writeto_mem(self.addr, 0x06, b'\x23')
        self.i2c.writeto_mem(self.addr, 0x07, b'\xa3')
        self.i2c.writeto_mem(self.addr, 0x09, b'\x00')
        self.i2c.writeto_mem(self.addr, 0x08, b'\x07')
        time.sleep_ms(60)

    def sxt(self, val, bits):
        if val & (1 << (bits - 1)):
            val -= (1 << bits)
        return val

    def read_coefficient(self):
        raw = self.i2c.readfrom_mem(self.addr, 0x10, 18)
        return {
            "c0": self.sxt((raw[0] << 4) | (raw[1] >> 4), 12),
            "c1": self.sxt(((raw[1] & 0x0F) << 8) | raw[2], 12),
            "c00": self.sxt((raw[3] << 12) | (raw[4] << 4) | (raw[5] >> 4), 20),
            "c10": self.sxt(((raw[5] & 0x0F) << 16) | (raw[6] << 8) | raw[7], 20),
            "c01": self.sxt((raw[8] << 8) | raw[9], 16),
            "c11": self.sxt((raw[10] << 8) | raw[11], 16),
            "c20": self.sxt((raw[12] << 8) | raw[13], 16),
            "c21": self.sxt((raw[14] << 8) | raw[15], 16),
            "c30": self.sxt((raw[16] << 8) | raw[17], 16),
        }

    def read_measurement(self):
        raw = self.i2c.readfrom_mem(self.addr, 0x00, 6)
        prs_raw = self.sxt((raw[0] << 16) | (raw[1] << 8) | raw[2], 24)
        tmp_raw = self.sxt((raw[3] << 16) | (raw[4] << 8) | raw[5], 24)
        p_sc = prs_raw / 7864320
        t_sc = tmp_raw / 7864320
        c = self.coef
        temp = c["c0"] / 2 + c["c1"] * t_sc
        pressure = (c["c00"] + p_sc * (c["c10"] + p_sc * (c["c20"] + p_sc * c["c30"]))
                    + t_sc * (c["c01"] + p_sc * (c["c11"] + p_sc * c["c21"])))
        return pressure / 100.0, temp

class RPR0521:
    def __init__(self, i2c):
        self.i2c = i2c
        self.addr = 0x38
        self.i2c.writeto(self.addr, bytes([0x42, 0x02]))
        self.i2c.writeto(self.addr, bytes([0x41, 0x86]))
        time.sleep_ms(100)

    def read(self):
        self.i2c.writeto(self.addr, bytes([0x46]), False)
        buf = self.i2c.readfrom(self.addr, 4)
        als0 = (buf[1] << 8) | buf[0]
        als1 = (buf[3] << 8) | buf[2]
        return als0, als1

    def lux_from_raw(self, als0, als1):
        if als0 < 1e-3:
            return 0.0
        r = als1 / als0
        if r < 0.595:
            return (1.682 * als0) - (1.877 * als1)
        elif r < 1.015:
            return (0.644 * als0) - (0.132 * als1)
        elif r < 1.352:
            return (0.756 * als0) - (0.243 * als1)
        elif r < 3.053:
            return (0.766 * als0) - (0.250 * als1)
        else:
            return 0.0

class BH1750:
    def __init__(self, i2c):
        self.i2c = i2c
        self.addr = 0x23
        self.i2c.writeto(self.addr, b'\x10')
        time.sleep_ms(180)

    def read_lux(self):
        data = self.i2c.readfrom(self.addr, 2)
        raw = (data[0] << 8) | data[1]
        return raw / 1.2

# ===== 初期化 =====
i2c = I2C(1, scl=Pin(22), sda=Pin(21))
scd41 = SCD41(i2c); scd41.init()
dps310 = DPS310(i2c)
rpr0521 = RPR0521(i2c)
bh1750 = BH1750(i2c)

BASE = "i483/sensors/" + STUDENT_ID + "/"

def topic(sensor: str, dtype: str) -> str:
    return BASE + sensor + "/" + dtype

print("Start publish loop (every", PUB_INTERVAL, "s)…")
next_t = time.ticks_ms()

while True:
    mqtt.check_msg()
    now = time.ticks_ms()

    if time.ticks_diff(now, next_t) >= 0:
        next_t = time.ticks_add(next_t, PUB_INTERVAL * 1000)

        if scd41.data_ready():
            co2, t_scd, h_scd = scd41.read_measurement()
            mqtt.publish(topic("SCD41", "co2"), f"{co2:.2f}")
            mqtt.publish(topic("SCD41", "temperature"), f"{t_scd:.2f}")
            mqtt.publish(topic("SCD41", "humidity"), f"{h_scd:.2f}")

        lux_bh = bh1750.read_lux()
        mqtt.publish(topic("BH1750", "illumination"), f"{lux_bh:.2f}")

        p, t_dp = dps310.read_measurement()
        mqtt.publish(topic("DPS310", "air_pressure"), f"{p:.2f}")
        mqtt.publish(topic("DPS310", "temperature"), f"{t_dp:.2f}")

        als0, als1 = rpr0521.read()
        ir_rpr = als1
        lux_rpr = rpr0521.lux_from_raw(als0, als1)
        mqtt.publish(topic("RPR0521", "infrared_illumination"), f"{ir_rpr:.2f}")
        mqtt.publish(topic("RPR0521", "illumination"), f"{lux_rpr:.2f}")

    if blink_mode and time.ticks_diff(now, last_toggle) >= BLINK_INTERVAL_MS:
        led.value(not led.value())
        last_toggle = now

    time.sleep_ms(20)
