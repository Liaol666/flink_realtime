package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink_PV_Wc10
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-07 11:23
 * @Version 1.0
 */
public class Flink_PV_Wc10 {
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
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriod(3);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs() * 1000L;
            }
        });
        SingleOutputStreamOperator<WaterSensor> waterDS = flatmapDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        waterDS.keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                       String msg = "当前key" + key + "窗口:[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "+ iterable.spliterator().estimateSize() + "条数据 ";
                       collector.collect(context.window().toString());
                       collector.collect(msg);
                    }
                });
        env.execute();
    }

    private static class MyPeriod implements WatermarkGenerator<WaterSensor> {
        private long maxTs = Long.MIN_VALUE;
        // 允许的最大延迟时间 ms
        private final long maxDelay;

        public MyPeriod(long maxDelay) {
            this.maxDelay = maxDelay * 1000;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }

        // 每收到一个元素, 执行一次. 用来生产WaterMark中的时间戳
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("onEvent..." + eventTimestamp);
            //有了新的元素找到最大的时间戳
            maxTs = Math.max(maxTs, eventTimestamp);
            System.out.println(maxTs);
        }

        // 周期性的把WaterMark发射出去, 默认周期是200ms
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

//            System.out.println("onPeriodicEmit...");
            // 周期性的发射水印: 相当于Flink把自己的时钟调慢了一个最大延迟
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1));
        }
    }
}
