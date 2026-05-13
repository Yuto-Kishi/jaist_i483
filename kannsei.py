import network
import socket
import utime
import machine
from machine import Pin, I2C, PWM

# --- 指定されたLCDライブラリクラス ---
class LCD:
    def __init__(self, i2c, addr=None, blen=1):
        self.bus = i2c
        self.addr = self.scanAddress(addr)
        self.blen = blen
        self.send_command(0x33)
        utime.sleep(0.005)
        self.send_command(0x32)
        utime.sleep(0.005)
        self.send_command(0x28)
        utime.sleep(0.005)
        self.send_command(0x0C)
        utime.sleep(0.005)
        self.send_command(0x01)
        self.bus.writeto(self.addr, bytearray([0x08]))

    def scanAddress(self, addr):
        devices = self.bus.scan()
        if len(devices) == 0:
            raise Exception("No LCD found")
        if addr is not None:
            if addr in devices: return addr
            else: raise Exception(f"LCD at 0x{addr:2X} not found")
        elif 0x27 in devices: return 0x27
        elif 0x3F in devices: return 0x3F
        else: raise Exception("No LCD found")

    def write_word(self, data):
        temp = data
        if self.blen == 1: temp |= 0x08
        else: temp &= 0xF7
        self.bus.writeto(self.addr, bytearray([temp]))

    def send_command(self, cmd):
        buf = cmd & 0xF0
        buf |= 0x04
        self.write_word(buf)
        utime.sleep(0.002)
        buf &= 0xFB
        self.write_word(buf)
        buf = (cmd & 0x0F) << 4
        buf |= 0x04
        self.write_word(buf)
        utime.sleep(0.002)
        buf &= 0xFB
        self.write_word(buf)

    def send_data(self, data):
        buf = data & 0xF0
        buf |= 0x05
        self.write_word(buf)
        utime.sleep(0.002)
        buf &= 0xFB
        self.write_word(buf)
        buf = (data & 0x0F) << 4
        buf |= 0x05
        self.write_word(buf)
        utime.sleep(0.002)
        buf &= 0xFB
        self.write_word(buf)

    def clear(self):
        self.send_command(0x01)

    def write(self, x, y, str_data):
        if x < 0: x = 0
        if x > 15: x = 15
        if y < 0: y = 0
        if y > 1: y = 1
        addr = 0x80 + 0x40 * y + x
        self.send_command(addr)
        for char in str_data:
            self.send_data(ord(char))

    def message(self, text):
        for char in text:
            if char == "\n":
                self.send_command(0xC0)
            else:
                self.send_data(ord(char))

# --- 設定項目 ---
SSID = "あなたのWiFi名"
PASSWORD = "あなたのパスワード"

# ハードウェア設定
pir = Pin(20, Pin.IN)
speaker = PWM(Pin(28))
i2c = I2C(0, sda=Pin(4), scl=Pin(5), freq=400000)

# LCD初期化
lcd = LCD(i2c)

# 記録用リスト
detection_log = []

# WiFi接続
def connect_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(SSID, PASSWORD)
    
    lcd.clear()
    lcd.write(0, 0, "WiFi Connecting")
    
    while not wlan.isconnected():
        utime.sleep(1)
    
    ip = wlan.ifconfig()[0]
    lcd.clear()
    lcd.write(0, 0, "Connected!")
    lcd.write(0, 1, ip)
    print("IP Address:", ip)
    utime.sleep(2)
    return ip

# HTML生成
def generate_html():
    rows = ""
    # 最新の50件を表示（メモリ保護のため）
    display_list = list(reversed(detection_log))[:50]
    for i, timestamp in enumerate(display_list):
        num = len(detection_log) - i
        rows += f"<tr><td>{num}</td><td>{timestamp}</td></tr>"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta http-equiv="refresh" content="5">
        <title>Pico W PIR Monitor</title>
        <style>
            body {{ font-family: sans-serif; text-align: center; background: #eee; }}
            table {{ margin: 20px auto; border-collapse: collapse; width: 60%; background: white; }}
            th, td {{ border: 1px solid #ccc; padding: 10px; }}
            th {{ background: #333; color: white; }}
        </style>
    </head>
    <body>
        <h1>Motion Detection Log</h1>
        <table>
            <tr><th>No.</th><th>Time (UpTime)</th></tr>
            {rows}
        </table>
    </body>
    </html>
    """
    return html

# サーバー準備
ip_addr = connect_wifi()
addr = socket.getaddrinfo('0.0.0.0', 80)[0][-1]
s = socket.socket()
s.bind(addr)
s.listen(1)
s.setblocking(False)

lcd.clear()
lcd.write(0, 0, "Monitoring...")

while True:
    # センサー検知
    if pir.value() == 1:
        # LCD表示
        lcd.clear()
        lcd.message("Motion Detected!\nEnglish Version")
        
        # 音
        speaker.freq(880)
        speaker.duty_u16(32768)
        utime.sleep(0.3)
        speaker.duty_u16(0)
        
        # 記録
        uptime = utime.ticks_ms() // 1000
        detection_log.append(f"{uptime} sec")
        
        utime.sleep(2) # チャタリング防止
        lcd.clear()
        lcd.write(0, 0, "Monitoring...")

    # Webサーバー処理
    try:
        cl, client_addr = s.accept()
        request = cl.recv(1024)
        response = generate_html()
        cl.send('HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n')
        cl.send(response)
        cl.close()
    except OSError:
        pass

    utime.sleep(0.1)
