package com.zll.flink.exer;

import akka.stream.impl.ReducerState;
import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @ClassName Flink_PV_Wc15
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 0:17
 * @Version 1.0
 */
public class Flink_PV_Wc15 {
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
        KeyedStream<WaterSensor, String> keyedStream = mapDS.keyBy(WaterSensor::getId);
        keyedStream.process(new MykeyedPro()).print();
        env.execute();
    }

    private static class MykeyedPro extends KeyedProcessFunction<String, WaterSensor, WaterSensor> {
        private ValueState<String> vs;
        private ListState<Long> ls;
        private MapState<String, Long> ms;
        private ReducingState<WaterSensor> rs;
        private AggregatingState<WaterSensor, WaterSensor> as;

        @Override
        public void open(Configuration conf) throws Exception {
            vs = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            ls = getRuntimeContext().getListState(new ListStateDescriptor<Long>("", Long.class));
            ms = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("", String.class, Long.class));
//            rs =getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("", ""));
//            as = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Object, WaterSensor>("", ))
        }
        @Override
        public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
            String id = waterSensor.getId();
            Long ts = waterSensor.getTs();
            Integer vc = waterSensor.getVc();
            vs.update(id);
            vs.clear();
            ls.add(ts);
            ls.update(new ArrayList<>());
            ls.clear();

         }
    }
}
