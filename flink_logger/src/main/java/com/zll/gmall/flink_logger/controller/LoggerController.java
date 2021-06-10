package com.zll.gmall.flink_logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName LoggerController
 * @Description TODO
 * @Author 17588
 * @Date 2021-05-17 14:28
 * @Version 1.0
 */
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/applog")

    public String getLogger(@RequestParam("param") String jsonstr) {
//        将数据落盘
        log.info(jsonstr);
//        将数据发送至kafka ODS主题
        kafkaTemplate.send("ods_base_log", jsonstr);
        return "success";
    }
}
