package com.zll.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName AdClickUserBehavior
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 13:04
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdClickUserBehavior {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
