package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName Flink_PV_Wc16
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 0:37
 * @Version 1.0
 */
public class Flink_PV_Wc16 {
    private static OutputTag<String> tag = new OutputTag<String>("side") {};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<WaterSensor, String> keyedDS = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                }).keyBy(WaterSensor::getId);
        SingleOutputStreamOperator<WaterSensor> result = keyedDS.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private ValueState<Integer> lastVc;
            private ValueState<Long> lastTs;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("lastTs", Long.class));
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class, Integer.MIN_VALUE));
            }

            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                Integer vc = lastVc.value();
                Long ts = lastTs.value();
                Integer curVc = waterSensor.getVc();
                if (curVc >= vc && ts == null) {
                    long tss = context.timerService().currentProcessingTime() + 10000l;
                    context.timerService().registerProcessingTimeTimer(tss);
                    lastTs.update(tss);
                } else if (curVc < vc && ts != null) {
                    context.timerService().deleteProcessingTimeTimer(ts);
                    lastTs.clear();
                }
                lastVc.update(curVc);
                collector.collect(waterSensor);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(tag, ctx.getCurrentKey() + "连续10s上升");
                lastTs.clear();
            }
        });
        result.print("main");
        result.getSideOutput(tag).print("side");
        env.execute();
    }
}
