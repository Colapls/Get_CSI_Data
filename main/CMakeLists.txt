idf_component_register(SRCS "main.c"
                       INCLUDE_DIRS "."
                       REQUIRES esp_wifi esp_event esp_netif nvs_flash lwip esp_timer esp_driver_gpio esp_driver_uart esp_http_server freertos esp_driver_rmt)
