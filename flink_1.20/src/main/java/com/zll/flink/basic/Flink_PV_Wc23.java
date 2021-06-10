package com.zll.flink.basic;

import com.zll.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName Flink_PV_Wc23
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-09 12:42
 * @Version 1.0
 */
public class Flink_PV_Wc23 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.readTextFile("flink_1.20/input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> filterDS  = inputDS.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
            }
        }).filter(data -> "pv".equals(data.getBehavior()));
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });
        filterDS.assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy)
        .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                return new Tuple2<>("PV", 1);
            }
        }).keyBy(data->data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum(1)
                .print();

        env.execute();
    }
}
