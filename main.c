#include <stdio.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/i2c.h"

#define I2C_MASTER_NUM           I2C_NUM_0
#define I2C_MASTER_SCL_IO        19         // SCL接続ピン
#define I2C_MASTER_SDA_IO        21         // SDA接続ピン
#define I2C_MASTER_FREQ_HZ       100000
#define I2C_MASTER_TX_BUF_DISABLE 0
#define I2C_MASTER_RX_BUF_DISABLE 0

#define BH1750_ADDR              0x23       // 通常アドレス（0x5Cの可能性もあり）
#define BH1750_CMD_CONT_H_RES_MODE 0x10     // 連続・高分解能モード

// 初期化：BH1750に測定モードを送信
esp_err_t bh1750_init() {
    i2c_cmd_handle_t cmd = i2c_cmd_link_create();
    i2c_master_start(cmd);
    i2c_master_write_byte(cmd, (BH1750_ADDR << 1) | I2C_MASTER_WRITE, true);
    i2c_master_write_byte(cmd, BH1750_CMD_CONT_H_RES_MODE, true);
    i2c_master_stop(cmd);
    esp_err_t ret = i2c_master_cmd_begin(I2C_MASTER_NUM, cmd, pdMS_TO_TICKS(1000));
    i2c_cmd_link_delete(cmd);
    return ret;
}

// 照度データ（2バイト）を取得して結合
uint16_t bh1750_read_lux() {
    uint8_t data[2] = {0};
    i2c_cmd_handle_t cmd = i2c_cmd_link_create();
    i2c_master_start(cmd);
    i2c_master_write_byte(cmd, (BH1750_ADDR << 1) | I2C_MASTER_READ, true);
    i2c_master_read(cmd, data, 2, I2C_MASTER_LAST_NACK);
    i2c_master_stop(cmd);
    esp_err_t ret = i2c_master_cmd_begin(I2C_MASTER_NUM, cmd, pdMS_TO_TICKS(1000));
    i2c_cmd_link_delete(cmd);

    if (ret == ESP_OK) {
        return (data[0] << 8) | data[1];  // 照度(Lux)
    } else {
        return 0xFFFF;  // 読み取り失敗時
    }
}

void app_main(void) {
    // I2C構成
    i2c_config_t conf = {
        .mode = I2C_MODE_MASTER,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_pullup_en = GPIO_PULLUP_ENABLE,
        .scl_pullup_en = GPIO_PULLUP_ENABLE,
        .master.clk_speed = I2C_MASTER_FREQ_HZ
    };
    i2c_param_config(I2C_MASTER_NUM, &conf);
    i2c_driver_install(I2C_MASTER_NUM, conf.mode,
                       I2C_MASTER_RX_BUF_DISABLE,
                       I2C_MASTER_TX_BUF_DISABLE, 0);

    // BH1750初期化
    if (bh1750_init() == ESP_OK) {
        printf("BH1750 初期化成功\n");
    } else {
        printf("BH1750 初期化失敗\n");
        return;
    }

    // 照度の取得ループ
    while (1) {
        uint16_t lux = bh1750_read_lux();
        if (lux == 0xFFFF) {
            printf("照度読み取り失敗\n");
        } else {
            printf("照度: %u lux\n", lux);
        }
        vTaskDelay(pdMS_TO_TICKS(1000));  // 1秒待機
    }
}