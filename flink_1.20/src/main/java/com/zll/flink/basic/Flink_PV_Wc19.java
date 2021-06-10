package com.zll.flink.basic;

import com.zll.flink.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName Flink_PV_Wc19
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 14:02
 * @Version 1.0
 */
public class Flink_PV_Wc19 {
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
        keybyDS.process(new KeyedProcessFunction<String, WaterSensor, List<Integer>>() {
            private ListState<Integer> lsState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lsState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("lsState", Integer.class));
            }
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<List<Integer>> collector) throws Exception {
                lsState.add(waterSensor.getVc());
                ArrayList<Integer> integers = Lists.newArrayList(lsState.get().iterator());
                integers.sort((x1, x2)-> x2 - x1);
                if (integers.size() > 3) {
                    integers.remove(3);
                }
                lsState.update(integers);
                collector.collect(integers);
            }
        }).print();
        keybyDS.process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
            private ReducingState<Integer> sumVcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                sumVcState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("", Integer::sum, Integer.class));
            }

            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<Integer> collector) throws Exception {
                sumVcState.add(waterSensor.getVc());
                collector.collect(sumVcState.get());
            }
        }).print();
        keybyDS.process(new KeyedProcessFunction<String, WaterSensor, Double>() {
            private AggregatingState<Integer, Double> avgState;

            @Override
            public void open(Configuration parameters) throws Exception {
                avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor
                        <Integer, Tuple2<Integer, Integer>, Double>("avgState",
                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }
                    @Override
                    public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> tuple2) {
                        return Tuple2.of(tuple2.f0 + integer, tuple2.f1 + 1);
                    }
                    @Override
                    public Double getResult(Tuple2<Integer, Integer> tuple2) {
                        return tuple2.f0 * 1D / tuple2.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                        return Tuple2.of(integerIntegerTuple2.f0 + acc1.f0, integerIntegerTuple2.f1 + acc1.f1);
                    }
                }, Types.TUPLE(Types.INT, Types.INT)));
            }
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<Double> collector) throws Exception {
                avgState.add(waterSensor.getVc());
                collector.collect(avgState.get());
            }
        }).print();
        keybyDS.process(new KeyedProcessFunction<String, WaterSensor, Double>() {
            private AggregatingState<Integer, Double> aggState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>
                        ("aggState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0, 0);
                            }
                            @Override
                            public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> tuple2) {
                                return Tuple2.of(tuple2.f0 + integer, tuple2.f1 + 1);
                            }
                            @Override
                            public Double getResult(Tuple2<Integer, Integer> tuple2) {
                                return tuple2.f0 * 1D / tuple2.f1;
                            }
                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> tuple2, Tuple2<Integer, Integer> acc1) {
                                return Tuple2.of(tuple2.f0 + acc1.f0 , tuple2.f1 + acc1.f1);
                            }
                        }, Types.TUPLE(Types.INT, Types.INT)));
            }
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<Double> collector) throws Exception {
                aggState.add(waterSensor.getVc());
                collector.collect(aggState.get());
            }
        }).print();
        keybyDS.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private MapState<Integer, String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("mapState", Integer.class, String.class));
            }
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                if (!mapState.contains(waterSensor.getVc())) {
                    collector.collect(waterSensor);
                    mapState.put(waterSensor.getVc(), waterSensor.getId());
                }
            }
        }).print();
        env.execute();

    }
}
