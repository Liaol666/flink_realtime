package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;

/**
 * @ClassName Flink_PV_Wc14
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-07 22:38
 * @Version 1.0
 */
public class Flink_PV_Wc14 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> mapDS = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });
        SingleOutputStreamOperator<WaterSensor> result = mapDS.keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private Integer lastvc = Integer.MIN_VALUE;
            private Long lastts = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
//               注册定时器
                Integer vc = waterSensor.getVc();
                if (vc >= lastvc && lastts == Long.MIN_VALUE) {
                    long ts = context.timerService().currentProcessingTime();
                    context.timerService().registerProcessingTimeTimer(ts + 5000L);
                    lastts = ts;
                } else if (vc < lastvc) {
//                删除定时器
                    context.timerService().deleteProcessingTimeTimer(lastts);
                    lastts = Long.MIN_VALUE;
                }
                lastvc = vc;
                collector.collect(waterSensor);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("sideOut") {
                }, ctx.getCurrentKey() + "连续10s");
                lastts = Long.MIN_VALUE;
            }
        });
        result.print();
        result.getSideOutput(new OutputTag<String>("sideOut"){}).print();

        env.execute();

    }
}
