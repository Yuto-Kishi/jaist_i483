from machine import I2C, Pin
import time
import struct

# SCD41 の I2C アドレス（固定）
SCD41_ADDR = 0x62

# 初期化（GPIO32 = SDA, GPIO33 = SCL）
i2c = I2C(0, scl=Pin(19), sda=Pin(21), freq=100000)

# CRC 計算（Sensirion の仕様に基づく）
def crc8(data):
    crc = 0xFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc << 1) ^ 0x31 if crc & 0x80 else crc << 1
            crc &= 0xFF
    return crc

# コマンド送信
def write_cmd(cmd):
    i2c.writeto(SCD41_ADDR, bytes(cmd))

# 測定値の読み取りコマンド送信 → 9バイト取得
def read_measurement():
    write_cmd([0xEC, 0x05])  # read measurement command
    time.sleep_ms(1)
    data = i2c.readfrom(SCD41_ADDR, 9)

    # CRCチェック
    for i in range(0, 9, 3):
        if crc8(data[i:i+2]) != data[i+2]:
            print("CRC error!")
            return None

    co2_raw = struct.unpack(">H", data[0:2])[0]
    temp_raw = struct.unpack(">H", data[3:5])[0]
    hum_raw = struct.unpack(">H", data[6:8])[0]

    temp = -45 + 175 * (temp_raw / 65535)
    hum = 100 * (hum_raw / 65535)
    return co2_raw, temp, hum

# 初期化と測定開始
def init_scd41():
    # Stop measurement
    write_cmd([0x3F, 0x86])
    time.sleep(1)
    # Start periodic measurement
    write_cmd([0x21, 0xB1])
    time.sleep(5)  # 最初のデータが出るまで待機

init_scd41()

# 測定ループ
while True:
    write_cmd([0xE4, 0xB8])  # Data ready?
    time.sleep_ms(1)
    ready = i2c.readfrom(SCD41_ADDR, 3)
    if ready[1] & 0x07:  # データ準備完了（11bit中1以上）
        result = read_measurement()
        if result:
            co2, temp, hum = result
            print("CO2: {} ppm, Temp: {:.2f} °C, Hum: {:.2f} %".format(co2, temp, hum))
    time.sleep(5)

