package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink_PV_Wc13
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-07 22:04
 * @Version 1.0
 */
public class Flink_PV_Wc13 {
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
        mapDS.keyBy(WaterSensor::getId).process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                long ts = context.timerService().currentProcessingTime();
//                context.timerService().currentWatermark()u
                System.out.println(ts);
                context.timerService().registerProcessingTimeTimer(ts + 1000L);
                collector.collect(waterSensor);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("定时器触发：" + timestamp);
                long ts = ctx.timerService().currentProcessingTime();
                System.out.println(ts);
                ctx.timerService().registerProcessingTimeTimer(ts + 1000L);

            }
        }).print();
        env.execute();
    }
}
