package com.example.modul;

import lombok.*;

import java.io.Serializable;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SensorReading implements Serializable {

    private static final long serialVersionUID = -8266604318764952290L;

    /*
            传感器id
         */
    private String sensorId;

    /*
        传感器时间戳
     */
    private Long timestamp;

    /*
        传感器温度
     */
    private Double temperature;

}