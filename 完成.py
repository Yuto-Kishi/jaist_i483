import network
import socket
import utime
from machine import Pin, I2C, PWM
from pico_i2c_lcd import I2cLcd # 事前にライブラリのインストールが必要です

# --- 設定 ---
SSID = "あなたのWiFi名"
PASSWORD = "あなたのパスワード"

# ピン割り当て
pir = Pin(20, Pin.IN)
speaker = PWM(Pin(28))
i2c = I2C(0, sda=Pin(4), scl=Pin(5), freq=400000)

# LCD初期化 (アドレス 0x27)
lcd = I2cLcd(i2c, 0x27, 2, 16)

# 検知データを格納するリスト
detection_log = []

# --- WiFi接続 ---
def connect_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(SSID, PASSWORD)
    
    lcd.clear()
    lcd.putstr("Connecting...")
    
    while not wlan.isconnected():
        utime.sleep(1)
        
    ip = wlan.ifconfig()[0]
    print(f"Connected! IP: {ip}")
    lcd.clear()
    lcd.putstr("IP Address:\n" + ip)
    utime.sleep(2)
    return ip

# --- HTML生成 ---
def generate_html():
    # リストを逆順にして、新しい検知が一番上にくるようにする
    rows = ""
    for i, timestamp in enumerate(reversed(detection_log)):
        rows += f"<tr><td>{len(detection_log)-i}</td><td>{timestamp}</td></tr>"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta http-equiv="refresh" content="5">
        <title>Pico W Monitor</title>
        <style>
            body {{ font-family: Arial; text-align: center; background: #f4f4f4; }}
            table {{ margin: 20px auto; border-collapse: collapse; width: 80%; background: white; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; }}
            th {{ background-color: #007bff; color: white; }}
            .status {{ color: #28a745; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>PIR Sensor Log</h1>
        <p class="status">Monitoring active...</p>
        <table>
            <tr><th>No.</th><th>Detection Time (Up Time)</th></tr>
            {rows}
        </table>
    </body>
    </html>
    """
    return html

# --- メイン処理 ---
ip = connect_wifi()

# Webサーバーの準備
addr = socket.getaddrinfo('0.0.0.0', 80)[0][-1]
s = socket.socket()
s.bind(addr)
s.listen(1)
s.setblocking(False) # センサー監視を止めないためにノンブロッキングに設定

lcd.clear()
lcd.putstr("Scanning...")

while True:
    # 1. PIRセンサーの検知
    if pir.value() == 1:
        # LCD表示
        lcd.clear()
        lcd.putstr("Motion Detected!")
        
        # 音を鳴らす (1000Hzで0.3秒)
        speaker.freq(1000)
        speaker.duty_u16(32768)
        utime.sleep(0.3)
        speaker.duty_u16(0)
        
        # リストに時間を記録 (起動からの秒数)
        uptime_sec = utime.ticks_ms() // 1000
        detection_log.append(f"{uptime_sec} sec")
        
        # 連続検知を避けるための待機
        utime.sleep(2)
        lcd.clear()
        lcd.putstr("Scanning...")

    # 2. Webサーバーへのリクエスト処理
    try:
        cl, addr_client = s.accept()
        request = cl.recv(1024)
        
        # レスポンス送信
        response = generate_html()
        cl.send('HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n')
        cl.send(response)
        cl.close()
    except OSError:
        # リクエストがない場合はここを通る
        pass

    utime.sleep(0.1)