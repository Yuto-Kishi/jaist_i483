import struct
import sys
import time

from machine import I2C, Pin

# Common I2C Initialization
i2c = I2C(0, scl=Pin(19), sda=Pin(21), freq=100000) # 100kHz for all sensors

# --- SCD41 CO2 Sensor ---
SCD41_ADDR = 0x62

def scd41_crc8(data):
    crc = 0xFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc << 1) ^ 0x31 if crc & 0x80 else crc << 1
            crc &= 0xFF
    return crc

def scd41_write_cmd(cmd_bytes): # Changed name to cmd_bytes for clarity
    try:
        i2c.writeto(SCD41_ADDR, bytes(cmd_bytes))
        return True
    except OSError as e:
        print(f"SCD41 I2C write error: {e}")
        return False

def scd41_init():
    print("Initializing SCD41...")
    if not scd41_write_cmd([0x3F, 0x86]): return False # Stop periodic measurement
    time.sleep_ms(500)
    if not scd41_write_cmd([0x21, 0xB1]): return False # Start periodic measurement
    time.sleep_ms(20) # Command execution time
    print("SCD41 Initialized.")
    return True

def scd41_read_measurement():
    # Check if data is ready
    if not scd41_write_cmd([0xE4, 0xB8]): return None # Send "data ready status" command
    time.sleep_ms(1)
    try:
        ready_status = i2c.readfrom(SCD41_ADDR, 3)
    except OSError as e:
        print(f"SCD41 I2C read (data ready status) error: {e}")
        return None

    # The last 11 bits of the first two bytes are 0 if no data is ready.
    # Or, if an error occurred during the read, the status might be invalid.
    # CRC check for ready_status itself
    if scd41_crc8(ready_status[0:2]) != ready_status[2]:
        print("SCD41 Data ready status CRC error!")
        return None

    if (ready_status[0] == 0x00 and ready_status[1] == 0x00): # No data available yet
        # print("SCD41: Data not ready yet.") # Optional debug
        return None

    # If data is ready, read measurement data
    if not scd41_write_cmd([0xEC, 0x05]): return None # Send "read measurement" command
    time.sleep_ms(1)
    try:
        data = i2c.readfrom(SCD41_ADDR, 9)
    except OSError as e:
        print(f"SCD41 I2C read (measurement) error: {e}")
        return None

    # CRC check for all three measurements
    if scd41_crc8(data[0:2]) != data[2] or \
       scd41_crc8(data[3:5]) != data[5] or \
       scd41_crc8(data[6:8]) != data[8]:
        print("SCD41 CRC error in measurement data!")
        return None

    co2_raw = struct.unpack(">H", data[0:2])[0]
    temp_raw = struct.unpack(">H", data[3:5])[0]
    hum_raw = struct.unpack(">H", data[6:8])[0]

    co2 = co2_raw
    temp = -45 + 175 * (temp_raw / 65535.0)
    hum = 100 * (hum_raw / 65535.0)
    return co2, temp, hum

# --- DPS310 Pressure & Temperature Sensor (Adhering to DPS310.py logic) ---
DPS310_ADDR  = 0x77
DPS310_R_RST, DPS310_R_ID, DPS310_R_COEF = 0x0C, 0x0D, 0x10
DPS310_R_PRS_CFG, DPS310_R_TMP_CFG = 0x06, 0x07
DPS310_R_CFG, DPS310_R_MEAS, DPS310_R_DATA = 0x09, 0x08, 0x00

DPS310_OSR   = 0b011 # ×8
DPS310_RATE  = 0x02  # 4 Hz
DPS310_K_VAL = 7_864_320 # K value from DPS310.py (Table 21 for OSR=3, SHIFT=0)

dps310_coefficients = {} # Store coefficients here

def dps310_sxt(v,b): return v-(1<<b) if v&(1<<(b-1)) else v

def dps310_read_coefficients_from_sensor(): # Renamed to avoid conflict if there was a global read_coef
    global dps310_coefficients
    try:
        d = i2c.readfrom_mem(DPS310_ADDR, DPS310_R_COEF, 18)
        dps310_coefficients = dict(
            c0  = dps310_sxt((d[0]<<4)|(d[1]>>4),12),
            c1  = dps310_sxt(((d[1]&0xF)<<8)|d[2],12),
            c00 = dps310_sxt((d[3]<<12)|(d[4]<<4)|(d[5]>>4),20),
            c10 = dps310_sxt(((d[5]&0xF)<<16)|(d[6]<<8)|d[7],20),
            c01 = dps310_sxt(d[8]<<8|d[9],16),
            c11 = dps310_sxt(d[10]<<8|d[11],16),
            c20 = dps310_sxt(d[12]<<8|d[13],16),
            c21 = dps310_sxt(d[14]<<8|d[15],16),
            c30 = dps310_sxt(d[16]<<8|d[17],16)
        )
        return True
    except OSError as e:
        print(f"DPS310: Failed to read coefficients: {e}")
        return False
    except Exception as e: # Catch other potential errors like unpacking
        print(f"DPS310: Error processing coefficients: {e}")
        return False


def dps310_init():
    print("Initializing DPS310...")
    global dps310_coefficients
    try:
        # Reset
        i2c.writeto_mem(DPS310_ADDR, DPS310_R_RST, b'\x89')
        time.sleep_ms(40) # Crucial delay after reset

        # Check Product ID
        product_id = i2c.readfrom_mem(DPS310_ADDR, DPS310_R_ID, 1)
        if product_id[0] != 0x10:
            print(f"DPS310: Unexpected Product ID: {hex(product_id[0])}. Expected 0x10.")
            return False

        # Read coefficients
        if not dps310_read_coefficients_from_sensor():
            return False

        # Configuration (matches DPS310.py)
        common_cfg = (DPS310_RATE << 4) | DPS310_OSR # 0x23 for Rate 4Hz, OSR x8
        i2c.writeto_mem(DPS310_ADDR, DPS310_R_PRS_CFG, bytes([common_cfg]))
        i2c.writeto_mem(DPS310_ADDR, DPS310_R_TMP_CFG, bytes([common_cfg | 0x80])) # TMP_EXT=1 for external sensor (as in original script)
        
        # Set CFG_REG: Shift = 0 (as in original script, SHIFT=0)
        i2c.writeto_mem(DPS310_ADDR, DPS310_R_CFG, bytes([0x00])) # No shifts

        # Set measurement mode: Continuous P and T
        i2c.writeto_mem(DPS310_ADDR, DPS310_R_MEAS, b'\x07') # 0b00000111
        time.sleep_ms(60) # Wait for first measurement completion (as in original script)
        
        print("DPS310 Initialized.")
        return True
    except OSError as e:
        print(f"DPS310: Initialization I2C error: {e}")
        return False
    except Exception as e:
        print(f"DPS310: Generic initialization error: {e}")
        return False


def dps310_read_data():
    global dps310_coefficients
    if not dps310_coefficients: # Check if coefficients were loaded
        print("DPS310: Coefficients not loaded, cannot read data.")
        return None, None
    try:
        # Check data ready flags (optional but good practice, original script does not explicitly poll these before read)
        # meas_status = i2c.readfrom_mem(DPS310_ADDR, DPS310_R_MEAS, 1)
        # if not (meas_status[0] & 0x10): # TMP_RDY
        #     print("DPS310 Temp data not ready")
        # if not (meas_status[0] & 0x20): # PRS_RDY
        #     print("DPS310 Pressure data not ready")

        raw_bytes = i2c.readfrom_mem(DPS310_ADDR, DPS310_R_DATA, 6)
        
        prs_raw = dps310_sxt((raw_bytes[0] << 16) | (raw_bytes[1] << 8) | raw_bytes[2], 24)
        tmp_raw = dps310_sxt((raw_bytes[3] << 16) | (raw_bytes[4] << 8) | raw_bytes[5], 24)

        t_sc = tmp_raw / DPS310_K_VAL
        p_sc = prs_raw / DPS310_K_VAL
        
        cf = dps310_coefficients # Use shorter alias
        temp_c = cf['c0'] * 0.5 + cf['c1'] * t_sc
        pressure_hpa = (cf['c00'] +
                        p_sc * (cf['c10'] + p_sc * (cf['c20'] + p_sc * cf['c30'])) +
                        t_sc * cf['c01'] +
                        t_sc * p_sc * (cf['c11'] + p_sc * cf['c21'])) / 100.0
        return pressure_hpa, temp_c
    except OSError as e:
        print(f"DPS310: I2C read error during data acquisition: {e}")
        return None, None
    except Exception as e:
        print(f"DPS310: Error processing data: {e}")
        return None, None

# --- RPR-0521RS Proximity & Ambient Light Sensor (Adhering to RPR.py logic) ---
RPR_ADDR = 0x38
RPR_REG_MANUFACT_ID = 0x92
RPR_REG_MODE_CONTROL = 0x41
RPR_REG_ALS_PS_CONTROL = 0x42
RPR_DATA_REG_START = 0x44 # Starting register for ALS data in RPR.py logic

# Constants from RPR.py
RPR_ALS_GAIN_BITS = 0x05     # ALS Gain x2, PS Gain x4
RPR_MODE_CONTROL_BITS = 0xC6 # ALS/PS Enable, 100ms measurement time

# Helper functions from RPR.py (crucial for its I2C communication pattern)
def rpr_i2c_wr(reg, val):
    global i2c # Ensure global i2c is used
    try:
        i2c.writeto(RPR_ADDR, bytes((reg, val))) # This is writeto(ADDR, buf) where buf is (reg, val)
        return True
    except OSError as e:
        print(f"RPR I2C write error to reg {hex(reg)}: {e}")
        return False

def rpr_i2c_rd(reg, n=1):
    global i2c # Ensure global i2c is used
    try:
        i2c.writeto(RPR_ADDR, bytes([reg])) # Send register address
        return i2c.readfrom(RPR_ADDR, n)    # Read data
    except OSError as e:
        print(f"RPR I2C read error from reg {hex(reg)}: {e}")
        return None

def rpr_init():
    print("Initializing RPR-0521RS...")
    time.sleep_ms(100) # Power stability wait from RPR.py
    
    mid_bytes = rpr_i2c_rd(RPR_REG_MANUFACT_ID, 1)
    if mid_bytes is None:
        print("RPR-0521RS: Failed to read Manufacturer ID.")
        return False
    if mid_bytes[0] != 0xE0:
        print(f"RPR-0521RS: Unexpected ID: {hex(mid_bytes[0]) if mid_bytes else 'Read Fail'}. Expected 0xE0.")
        return False

    if not rpr_i2c_wr(RPR_REG_ALS_PS_CONTROL, RPR_ALS_GAIN_BITS): return False
    if not rpr_i2c_wr(RPR_REG_MODE_CONTROL, RPR_MODE_CONTROL_BITS): return False
    
    time.sleep_ms(100) # Wait for measurement cycle to start (as in RPR.py after mode set)
    print("RPR-0521RS Initialized.")
    return True

def rpr_lux_from_raw(d0, d1): # Exactly as in RPR.py
    f0, f1 = d0 / 2.0, d1 / 2.0 # Division by 2.0 is key from original script
    if f0 < 1e-3: return 0.0
    r = f1 / f0
    if r < 0.595:  return 1.682 * f0 - 1.877 * f1
    if r < 1.015:  return 0.644 * f0 - 0.132 * f1
    if r < 1.352:  return 0.756 * f0 - 0.243 * f1
    if r < 3.053:  return 0.766 * f0 - 0.250 * f1
    return 0.0

def rpr_read_lux():
    # Reads 6 bytes starting from RPR_DATA_REG_START (0x44)
    # This matches `buf = i2c_rd(i2c, 0x44, 6)` from RPR.py
    buf = rpr_i2c_rd(RPR_DATA_REG_START, 6)
    if buf is None or len(buf) < 6: # Need at least 6 bytes for the original indexing
        print("RPR: Failed to read ALS data buffer.")
        return None

    # Byte indexing for als0 and als1 as in RPR.py:
    # als0 = buf[3]<<8 | buf[2]
    # als1 = buf[5]<<8 | buf[4]
    # This means:
    # buf[2] is ALS Data 0 LSB
    # buf[3] is ALS Data 0 MSB
    # buf[4] is ALS Data 1 LSB
    # buf[5] is ALS Data 1 MSB
    # Note: This indexing is unusual if RPR_DATA_REG_START (0x44) is truly ALS0_LSB.
    # However, we must stick to the working RPR.py logic.
    try:
        als0_val = (buf[3] << 8) | buf[2]
        als1_val = (buf[5] << 8) | buf[4]
    except IndexError:
        print(f"RPR: Index error when processing buffer of length {len(buf)}.")
        return None
        
    lux = rpr_lux_from_raw(als0_val, als1_val)
    return lux

# --- BH1750 Light Sensor ---
BH1750_ADDR = 0x23
BH1750_CMD_POWER_ON = 0x01
BH1750_CMD_RESET = 0x07
BH1750_CMD_CONT_H_RES_MODE = 0x10

def bh1750_write_cmd(cmd_byte):
    try:
        i2c.writeto(BH1750_ADDR, bytes([cmd_byte]))
        return True
    except OSError as e:
        print(f"BH1750 I2C write error: {e}")
        return False

def bh1750_init():
    print("Initializing BH1750...")
    if not bh1750_write_cmd(BH1750_CMD_POWER_ON): return False
    time.sleep_ms(5)
    if not bh1750_write_cmd(BH1750_CMD_RESET): return False
    time.sleep_ms(5)
    if not bh1750_write_cmd(BH1750_CMD_CONT_H_RES_MODE): return False
    time.sleep_ms(180) # Wait for first measurement (max 180ms for H-Res mode)
    print("BH1750 Initialized.")
    return True

def bh1750_read_lux():
    try:
        raw_data = i2c.readfrom(BH1750_ADDR, 2)
        lux_raw = (raw_data[0] << 8) | raw_data[1]
        lux = lux_raw / 1.2
        return lux
    except OSError as e:
        print(f"BH1750 I2C read error: {e}")
        return None

# --- Main Program ---
def main_loop():
    # Initialize all sensors
    scd41_active = scd41_init()
    dps310_active = dps310_init()
    rpr_active = rpr_init()
    bh1750_active = bh1750_init()

    print("\n--- Sensor Initialization Status ---")
    print(f"SCD41:     {'ACTIVE' if scd41_active else 'FAIL'}")
    print(f"DPS310:    {'ACTIVE' if dps310_active else 'FAIL'}")
    print(f"RPR0521RS: {'ACTIVE' if rpr_active else 'FAIL'}")
    print(f"BH1750:    {'ACTIVE' if bh1750_active else 'FAIL'}")
    print("----------------------------------\n")

    if not (scd41_active or dps310_active or rpr_active or bh1750_active):
        print("No sensors initialized successfully. Exiting.")
        return

    print("Starting sensor data acquisition loop (5 second interval)...")
    while True:
        print(f"--- Timestamp: {time.ticks_ms() / 1000.0:.2f} s ---") # Add a timestamp

        # Read SCD41
        if scd41_active:
            scd41_data = scd41_read_measurement()
            if scd41_data:
                co2, scd_temp, scd_hum = scd41_data
                print(f"SCD41: CO2: {co2} ppm, Temp: {scd_temp:.2f} C, Hum: {scd_hum:.2f} %")
            else:
                print("SCD41: No data or failed to read.")
        else:
            print("SCD41: Not active.")

        # Read DPS310
        if dps310_active:
            dps_data = dps310_read_data() # Returns tuple (pressure, temp) or (None, None)
            if dps_data and dps_data[0] is not None and dps_data[1] is not None:
                dps_press, dps_temp = dps_data
                print(f"DPS310: Pressure: {dps_press:.2f} hPa, Temp: {dps_temp:.2f} C")
            else:
                print("DPS310: Failed to read data.")
        else:
            print("DPS310: Not active.")

        # Read RPR-0521RS
        if rpr_active:
            rpr_lux_val = rpr_read_lux()
            if rpr_lux_val is not None:
                print(f"RPR0521RS: Ambient Light: {rpr_lux_val:.2f} lux")
            else:
                print("RPR0521RS: Failed to read data or Lux is 0.") # Clarify if None vs 0.0
        else:
            print("RPR0521RS: Not active.")

        # Read BH1750
        if bh1750_active:
            bh_lux_val = bh1750_read_lux()
            if bh_lux_val is not None:
                print(f"BH1750: Ambient Light: {bh_lux_val:.2f} lux")
            else:
                print("BH1750: Failed to read data.")
        else:
            print("BH1750: Not active.")

        print("--------------------------------------")
        time.sleep(5)

if __name__ == "__main__":
    try:
        print("Scanning I2C bus...")
        devices = i2c.scan()
        if devices:
            print("Detected I2C devices at addresses:")
            for device_addr in devices:
                print(f"- {hex(device_addr)}")
        else:
            print("No I2C devices found. Please check wiring and power to sensors.")
            # Consider not exiting immediately to allow partial functionality if some sensors are later detected
            # sys.exit("Exiting due to no I2C devices.")

        time.sleep(1) # Give a moment for user to see scan results
        main_loop()

    except KeyboardInterrupt:
        print("Program stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred in the main execution: {e}")
        # For MicroPython, a soft reset can sometimes help after fatal errors.
        # import machine
        # machine.soft_reset()