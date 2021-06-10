package com.zll.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @ClassName PageViewCount
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-09 17:29
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageViewCount {
    private String pv;
    private String time;
    private Integer count;
}
