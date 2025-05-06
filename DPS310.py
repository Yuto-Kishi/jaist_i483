from machine import I2C, Pin
import time, sys

# ───────── I²C & 基本設定 ──────────────────────────────
i2c   = I2C(0, scl=Pin(19), sda=Pin(21), freq=100_000)
ADDR  = 0x77            # scan で確認
INTVL = 1               # [s]

# ───────── レジスタ ───────────────────────────────────
R_RST, R_ID, R_COEF = 0x0C, 0x0D, 0x10
R_PRS_CFG, R_TMP_CFG = 0x06, 0x07
R_CFG, R_MEAS, R_DATA = 0x09, 0x08, 0x00

# ───────── OSR ×8, SHIFT=0, 外部温度ADC ───────────────
OSR   = 0b011               # ×8
SHIFT = 0
K     = 7_864_320           # Table 21 (OSR=3, SHIFT=0)
RATE  = 0x02                # 4 Hz

def sxt(v,b): return v-(1<<b) if v&(1<<b-1) else v       # 2's complement

def read_coef():
    d=i2c.readfrom_mem(ADDR,R_COEF,18)
    return dict(
        c0 = sxt((d[0]<<4)|(d[1]>>4),12),
        c1 = sxt(((d[1]&0xF)<<8)|d[2],12),
        c00= sxt((d[3]<<12)|(d[4]<<4)|(d[5]>>4),20),
        c10= sxt(((d[5]&0xF)<<16)|(d[6]<<8)|d[7],20),
        c01= sxt(d[8]<<8|d[9],16),
        c11= sxt(d[10]<<8|d[11],16),
        c20= sxt(d[12]<<8|d[13],16),
        c21= sxt(d[14]<<8|d[15],16),
        c30= sxt(d[16]<<8|d[17],16)
    )

def init():
    i2c.writeto_mem(ADDR,R_RST,b'\x89'); time.sleep_ms(40)
    if i2c.readfrom_mem(ADDR,R_ID,1)[0]!=0x10:
        raise RuntimeError("ID error")
    cf=read_coef()

    common=(RATE<<4)|OSR                 # 0x23 : RATE=4 Hz, OSR=×8
    i2c.writeto_mem(ADDR,R_PRS_CFG,bytes([common]))
    i2c.writeto_mem(ADDR,R_TMP_CFG,bytes([common|0x80]))  # ★ TMP_EXT=1
    i2c.writeto_mem(ADDR,R_CFG,bytes([SHIFT<<4]))
    i2c.writeto_mem(ADDR,R_MEAS,b'\x07')                  # Continuous
    time.sleep_ms(60)
    return cf

def read(cf):
    d=i2c.readfrom_mem(ADDR,R_DATA,6)
    pr=sxt(d[0]<<16|d[1]<<8|d[2],24)
    tr=sxt(d[3]<<16|d[4]<<8|d[5],24)
    p_sc=pr/K; t_sc=tr/K
    temp=cf['c0']/2 + cf['c1']*t_sc
    pres=(cf['c00']
          + p_sc*(cf['c10']+p_sc*(cf['c20']+p_sc*cf['c30']))
          + t_sc*(cf['c01']+p_sc*(cf['c11']+p_sc*cf['c21'])))/100
    return pres,temp

try:
    coef=init()
except Exception as e:
    sys.exit("Init fail:"+str(e))

print("DPS310 外部温度ADC / OSR×8")
try:
    while True:
        p,t=read(coef)
        print("気圧 {:7.2f} hPa | 温度 {:6.2f} °C".format(p,t))
        time.sleep(INTVL)
except KeyboardInterrupt:
    pass