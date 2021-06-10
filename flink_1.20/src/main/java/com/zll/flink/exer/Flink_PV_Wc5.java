package com.zll.flink.exer;

import com.zll.flink.bean.AdClickUserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.HashMap;

/**
 * @ClassName Flink_PV_Wc5
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 13:03
 * @Version 1.0
 */
public class Flink_PV_Wc5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.readTextFile("flink_1.20/input/AdClickLog.csv");
        SingleOutputStreamOperator<AdClickUserBehavior> adClickUserBehaviorSingleOutputStreamOperator = inputDS.flatMap(new FlatMapFunction<String, AdClickUserBehavior>() {
            @Override
            public void flatMap(String s, Collector<AdClickUserBehavior> collector) throws Exception {
                String[] split = s.split(",");
                AdClickUserBehavior adClickUserBehavior = new AdClickUserBehavior(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        split[2],
                        split[3],
                        Long.valueOf(split[4])
                );
                collector.collect(adClickUserBehavior);
            }
        });
        KeyedStream<AdClickUserBehavior, Tuple2<String, Long>> adClickUserBehaviorTuple2KeyedStream = adClickUserBehaviorSingleOutputStreamOperator.keyBy(new KeySelector<AdClickUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(AdClickUserBehavior adClickUserBehavior) throws Exception {
                return new Tuple2<String, Long>(adClickUserBehavior.getProvince(), adClickUserBehavior.getAdId());
            }
        });
        adClickUserBehaviorTuple2KeyedStream.process(new KeyedProcessFunction<Tuple2<String,Long>, AdClickUserBehavior, Tuple2<Tuple2<String, Long>, Long>>() {
            private HashMap<String, Long> hashMap = new HashMap<>();
            @Override
            public void processElement(AdClickUserBehavior adClickUserBehavior, Context context, Collector<Tuple2<Tuple2<String, Long>, Long>> collector) throws Exception {
                String hashKey = adClickUserBehavior.getProvince() + "-" + adClickUserBehavior.getAdId();
                Long count = hashMap.getOrDefault(hashKey, 0L);
                count++;
                collector.collect(new Tuple2<Tuple2<String, Long>, Long>(context.getCurrentKey(), count));
                hashMap.put(hashKey, count);
            }
        }).print();
        env.execute();

    }
}
