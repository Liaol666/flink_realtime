package com.zll.flink.exer;

import com.zll.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink_PV_Wc2
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 11:06
 * @Version 1.0
 */
public class Flink_PV_Wc2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.readTextFile("flink_1.20/input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = inputDS.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> collector) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4])
                );
                if ("pv".equals(userBehavior.getBehavior())) {
                    collector.collect(userBehavior);
                }
            }
        });
//        userBehaviorSingleOutputStreamOperator.keyBy(data->data.getUserId())
        KeyedStream<UserBehavior, String> keybyDS = userBehaviorSingleOutputStreamOperator.keyBy(data -> "pv");
//        keybyDS.print();
        keybyDS.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            Integer count = 0;
            @Override
            public void processElement(UserBehavior userBehavior, Context context, Collector<Integer> collector) throws Exception {
                count++;
                collector.collect(count);
            }
        }).print();
        env.execute("Flink_PV_Wc2");
    }
}
