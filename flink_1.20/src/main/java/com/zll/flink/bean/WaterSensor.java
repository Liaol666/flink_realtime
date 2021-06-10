package com.zll.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName WaterSensor
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-04 16:30
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}