package kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @ClassName KafkaProducerTest
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-04 14:29
 * @Version 1.0
 */
public class KafkaProducerTest {
    public static void main(String[] args) throws InterruptedException {
//        初始化参数
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("buffer.memory", "33554432");
        prop.put("compression.type", "lz4");
        prop.put("batch.size", "32768");
        prop.put("linger.ms", 100);
        prop.put("retries", 10);
        prop.put("retry.backoff.ms", 300);
        prop.put("request.required.acks", "1");
//        创建kafka对象及记录
     KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "message1");
//        异步发送消息
        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("消息发送成功");
                } else {
                    System.out.println("消息发送失败");
                }
            }
        });
        Thread.sleep(100 * 10);
//        关闭资源
        kafkaProducer.close();
    }

}
