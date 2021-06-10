package com.zll.gmall.flink_logger.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName FirstController
 * @Description TODO
 * @Author 17588
 * @Date 2021-05-17 11:16
 * @Version 1.0
 */
@RestController
public class FirstController {
    @RequestMapping("/testDemo")
//    public String test(@RequestParam("name") String name, @RequestParam("age") int age) {
    public String test1 (@RequestParam("name") String name) {
//    public String test(){
        System.out.println(name + ":" );
        return "hello ";
    }
}
