from machine import Pin, I2C
import time

# I2C初期化：GPIO21 = SDA, GPIO19 = SCL
i2c = I2C(0, scl=Pin(19), sda=Pin(21), freq=100000)

# BH1750のアドレスとコマンド
BH1750_ADDR = 0x23  # 一般的なデフォルト（0x5Cの場合もあり）
BH1750_CMD_CONT_H_RES_MODE = 0x10

# BH1750初期化（測定モード設定）
def bh1750_init():
    try:
        i2c.writeto(BH1750_ADDR, bytes([BH1750_CMD_CONT_H_RES_MODE]))
        return True
    except OSError as e:
        print("初期化エラー:", e)
        return False

# BH1750から照度データを読み取る（2バイト）
def bh1750_read_lux():
    try:
        time.sleep_ms(180)  # 測定待機時間（連続高分解能モード）
        data = i2c.readfrom(BH1750_ADDR, 2)
        lux = (data[0] << 8) | data[1]
        return lux / 1.2  # データシートに基づく補正（1 lx 単位）
    except OSError as e:
        print("読み取りエラー:", e)
        return None

# 実行本体
if bh1750_init():
    print("BH1750 初期化成功")
    while True:
        lux = bh1750_read_lux()
        if lux is None:
            print("照度読み取り失敗")
        else:
            print("照度: {:.2f} lux".format(lux))
        time.sleep(1)
else:
    print("BH1750 初期化失敗")
