package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName Flink_PV_Wc9
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-06 22:13
 * @Version 1.0
 */
public class Flink_PV_Wc9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> flatmapDS = inputDS.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
                String[] split = s.split(" ");
                collector.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
            }
        });
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> waterDS = flatmapDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        WindowedStream<WaterSensor, String, TimeWindow> lateDS = waterDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor>("late"));

        SingleOutputStreamOperator<WaterSensor> sumDS = lateDS.sum("vc");
        sumDS.print("main");
        sumDS.getSideOutput(new OutputTag<WaterSensor>("late")).print();


        env.execute();
    }
}
