package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName Flink_PV_Wc7
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-05 12:40
 * @Version 1.0
 */
public class Flink_PV_Wc7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment streamEnv = env.setParallelism(1);
        DataStreamSource<String> socketDS = streamEnv.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> mapDS = socketDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] words = s.split(" ");
                return new WaterSensor(words[0], Long.valueOf(words[1]), Integer.valueOf(words[2]));
            }
        });
//        new WatermarkStrategy<WaterSensor>() {
//            @Override
//            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                return new MyPeriod(3);
//            }
//        }
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long timeStamp) {
                        return waterSensor.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> waterDS = mapDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        waterDS.keyBy(data->data.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print();
        env.execute();
    }
}
