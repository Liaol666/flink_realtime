package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
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
 * @ClassName Flink_PV_Wc17
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 10:40
 * @Version 1.0
 */
public class Flink_PV_Wc17 {
    private static OutputTag<String> tag = new OutputTag<String>("side"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<WaterSensor, String> keybyDS = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                }).keyBy(WaterSensor::getId);
        SingleOutputStreamOperator<WaterSensor> result = keybyDS.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private ValueState<Integer> vcState;
            private ValueState<Long> tsState;

            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vcState", Integer.class));
                tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", Long.class));
            }

            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                Integer vc = vcState.value();
                Long ts = tsState.value();
                Integer curVC = waterSensor.getVc();
                if (vc != null) {
                    if (curVC >= vc && ts == null) {
                        long ets = context.timerService().currentProcessingTime() + 10000L;
                        context.timerService().registerProcessingTimeTimer(ets);
                        tsState.update(ets);
                    } else if (curVC < vc && ts != null) {
                        context.timerService().deleteProcessingTimeTimer(ts);
                        tsState.clear();
                    }
                }
                vcState.update(curVC);
                collector.collect(waterSensor);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(tag, ctx.getCurrentKey() + "连续10S水位上升");
                tsState.clear();
            }
        });
        result.print("main");
        result.getSideOutput(tag).print("side-");

        env.execute();
    }
}
