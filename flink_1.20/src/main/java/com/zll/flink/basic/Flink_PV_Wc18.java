package com.zll.flink.basic;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink_PV_Wc18
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 12:41
 * @Version 1.0
 */
public class Flink_PV_Wc18 {
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
            keybyDS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
                private ValueState<Integer> vcState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vcState", Integer.class));
                }
                @Override
                public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) throws Exception {
                    Integer vc = vcState.value();
                    Integer curVc = waterSensor.getVc();
                    if (vc != null && curVc - vc >= 10) {
                        collector.collect(waterSensor.getId() + "水位线出现跳变");
                    }
                    vcState.update(curVc);
                }
            }).print();
            env.execute();
        }
}
