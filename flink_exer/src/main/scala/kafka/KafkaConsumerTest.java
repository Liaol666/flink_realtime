package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName KafkaConsumerTest
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-04 15:15
 * @Version 1.0
 */
public class KafkaConsumerTest {
    public static void main(String[] args) {
//        创建消费者和消费者组的信息
        String topicName = "test";
        String groupId = "testtest";
//        注册配置信息
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "hadoop102:9092");
        prop.put("group.id", groupId);
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.Stringdeserializer");
        prop.put("value.deserializer","org.apache.kafka.common.serialization.Stringdeserializer");
//      创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
//        指定消费主题
        consumer.subscribe(Arrays.asList(topicName));
//        去服务端消费数据
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.offset() + ", " + record.key() + ", " + record.value());
                }
            }
        }catch (Exception e) {

        }
    }
}
