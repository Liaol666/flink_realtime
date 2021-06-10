package com.zll.flink.exer;

import com.zll.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Types;

/**
 * @ClassName Flink_PV_Wc1
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 10:36
 * @Version 1.0
 */
public class Flink_PV_Wc1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.readTextFile("flink_1.20/input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> UserBehaviorDS = inputDS.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> collector) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4]));
                if ("pv".equals(userBehavior.getBehavior())) {
                    collector.collect(userBehavior);
                }
            }
        });
        UserBehaviorDS.map(data-> Tuple2.of("pv", 1L))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(org.apache.flink.api.common.typeinfo.Types.STRING, org.apache.flink.api.common.typeinfo.Types.LONG))
                .keyBy(data->data.f0)
                .sum(1)
                .print();
        System.out.println("============");
//        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = inputDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] split = s.split(",");
//                UserBehavior userBehavior = new UserBehavior(
//                        Long.valueOf(split[0]),
//                        Long.valueOf(split[1]),
//                        Integer.valueOf(split[2]),
//                        split[3],
//                        Long.valueOf(split[4]));
//                if ("pv".equals(userBehavior.getBehavior())) {
//                    collector.collect(new Tuple2<String, Integer>("pv", 1));
//                }
//            }
//        });
//        pv.keyBy(data-> data.f0)
//                .sum(1)
//                .print();

        env.execute();
    }
}
