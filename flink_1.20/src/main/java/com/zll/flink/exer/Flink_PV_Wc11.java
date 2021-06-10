package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

/**
 * @ClassName Flink_PV_Wc11
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-07 15:03
 * @Version 1.0
 */
public class Flink_PV_Wc11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> flatmapDS = inputDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        });
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        return waterSensor.getTs() * 1000;
                    }
                });
        OutputTag<WaterSensor> tag = new OutputTag<WaterSensor>("late"){};
        SingleOutputStreamOperator<WaterSensor> waterDS = flatmapDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        SingleOutputStreamOperator<WaterSensor> sumDS = waterDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(tag)
                .process(new ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<WaterSensor> collector) throws Exception {
                        String msg = "当前key：" + key + " 窗口:[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ")一共有" + elements.spliterator().estimateSize() + "条数据" + "watermark:" + context.toString();
//                        collector.collect(context.window().toString());
//                        Iterator<WaterSensor> iterator = elements.iterator();
//                        while (iterator.hasNext()) {
                            collector.collect(new WaterSensor(msg, elements.iterator().next().getTs(), elements.iterator().next().getVc()));
//                        }
                    }
                });
        sumDS.print("main");
        sumDS.getSideOutput(tag).print("sideOutput");
        env.execute();
    }
}
