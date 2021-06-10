package com.zll.flink.exer;

import com.zll.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @ClassName Flink_PV_Wc3
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 11:15
 * @Version 1.0
 */
public class Flink_PV_Wc3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.readTextFile("flink_1.20/input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = inputDS.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String s, Collector<UserBehavior> collector) throws Exception {
                String[] split = s.split(",");
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

        KeyedStream<UserBehavior, String> userBehaviorStringKeyedStream = userBehaviorSingleOutputStreamOperator.keyBy(data -> "UV");
        userBehaviorStringKeyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            private HashSet<Long> userIds = new HashSet();
            private Integer count = 0;
            @Override
            public void processElement(UserBehavior userBehavior, Context context, Collector<Integer> collector) throws  Exception {
                if (!userIds.contains(userBehavior.getUserId())) {
                    userIds.add(userBehavior.getUserId());
                    count++;
                }
                collector.collect(count);
            }
        }).print();
        env.execute();
    }
}
