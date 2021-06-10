package com.zll.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

public class MyKafkaUtil {
    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static Properties prop = new Properties();
    //封装Kafka消费者
    static {
        prop.setProperty("bootstrap.servers", kafkaServer);
    }
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){

        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
    }
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), prop );
    }
}