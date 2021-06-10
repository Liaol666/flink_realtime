package com.zll.flink.basic;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName Flink_PV_Wc22
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 22:34
 * @Version 1.0
 */
public class Flink_PV_Wc22 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> mapDS = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs() * 1000L;
            }
        });
        SingleOutputStreamOperator<WaterSensor> waterDS = mapDS.assignTimestampsAndWatermarks(watermarkStrategy);
        KeyedStream<Tuple2<String, Integer>, String> keybyDS = waterDS.map(new MapFunction<WaterSensor, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WaterSensor waterSensor) throws Exception {
                return new Tuple2<>(waterSensor.getId(), 1);
            }
        }).keyBy(data -> data.f0);
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("side") {
        };
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keybyDS.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .sum(1);
        sum.print("main");
        sum.getSideOutput(outputTag).print("side--");
        env.execute();

    }
}
