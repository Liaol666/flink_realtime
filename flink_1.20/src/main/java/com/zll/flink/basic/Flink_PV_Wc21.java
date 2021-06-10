package com.zll.flink.basic;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink_PV_Wc21
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 17:36
 * @Version 1.0
 */
public class Flink_PV_Wc21 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8082/"));
        env.enableCheckpointing(1000L);
        env.getCheckpointingMode();
        System.setProperty("HADOOP_USER_NAME", "zll");

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        env.execute();

    }
}
