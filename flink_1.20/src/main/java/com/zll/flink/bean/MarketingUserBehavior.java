package com.zll.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName MarketingUserBehavior
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 12:31
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
