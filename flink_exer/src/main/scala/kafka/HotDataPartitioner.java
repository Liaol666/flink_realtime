package kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @ClassName HotDataPartitioner
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-04 15:05
 * @Version 1.0
 */
public class HotDataPartitioner implements Partitioner {
    private Random random;
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String keys = (String)key;
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        int partitionCount = partitionInfos.size();
        int hotDataPartition = partitionCount - 1;
        return !keys.contains("hot_data") ? random.nextInt(partitionCount - 1):hotDataPartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        random = new Random();
    }
}
