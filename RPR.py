from machine import I2C, Pin
import time, sys

PIN_SDA, PIN_SCL = 21, 19
I2C_FREQ_HZ       = 100_000    # ← まず 100 kHz に下げる
ADDR              = 0x38
REG_MANUFACT_ID   = 0x92
REG_ALS_PS_CONTROL= 0x42
REG_MODE_CONTROL  = 0x41
ALS_GAIN_BITS     = 0x05
MODE_CONTROL_BITS = 0xC6

def i2c_wr(i2c, reg, val):
    i2c.writeto(ADDR, bytes((reg, val)))

def i2c_rd(i2c, reg, n=1):
    # STOP を入れて安全側に
    i2c.writeto(ADDR, bytes([reg]))
    return i2c.readfrom(ADDR, n)

def lux_from_raw(d0, d1):
    f0, f1 = d0/2, d1/2
    if f0 < 1e-3: return 0
    r = f1/f0
    if r < 0.595:  return 1.682*f0 - 1.877*f1
    if r < 1.015:  return 0.644*f0 - 0.132*f1
    if r < 1.352:  return 0.756*f0 - 0.243*f1
    if r < 3.053:  return 0.766*f0 - 0.250*f1
    return 0

def main():
    i2c = I2C(0, scl=Pin(PIN_SCL), sda=Pin(PIN_SDA), freq=I2C_FREQ_HZ)

    time.sleep_ms(100)                          # ★ 電源安定待ち
    mid = i2c_rd(i2c, REG_MANUFACT_ID)[0]
    print("ID read:", hex(mid))
    if mid != 0xE0:
        sys.exit("Unexpected ID")

    i2c_wr(i2c, REG_ALS_PS_CONTROL, ALS_GAIN_BITS)
    i2c_wr(i2c, REG_MODE_CONTROL,   MODE_CONTROL_BITS)

    while True:
        buf = i2c_rd(i2c, 0x44, 6)
        als0 = buf[3]<<8 | buf[2]
        als1 = buf[5]<<8 | buf[4]
        print("lux = %.2f" % lux_from_raw(als0, als1))
        time.sleep(1)

main()
