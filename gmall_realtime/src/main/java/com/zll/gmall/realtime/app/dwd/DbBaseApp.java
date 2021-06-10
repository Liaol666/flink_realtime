package com.zll.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.zll.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName DbBaseApp
 * @Description TODO
 * @Author 17588
 * @Date 2021-05-29 20:59
 * @Version 1.0
 */
public class DbBaseApp {
    public static void main(String[] args) throws Exception {
//        获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/dwd_log_ck"));

        System.setProperty("HADOOP_USER_NAME", "zll");
//        读取kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_db_m", "ods_db_group");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
//        将每行数据转换成ＪＳＯＮ对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(data -> JSONObject.parseObject(data));

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String data = value.getString("data");
                return data != null && data.length() > 0;
            }
        });

        filterDS.print();
        env.execute("dbBaseApp");

    }
}
