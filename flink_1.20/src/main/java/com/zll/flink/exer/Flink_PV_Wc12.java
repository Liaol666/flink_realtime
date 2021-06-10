package com.zll.flink.exer;

import com.zll.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

/**
 * @ClassName Flink_PV_Wc12
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-07 20:48
 * @Version 1.0
 */
public class Flink_PV_Wc12 {
    private static OutputTag<Tuple2> outtag = new OutputTag<Tuple2>("out"){};
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> mapDS = inputDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        });
        SingleOutputStreamOperator<WaterSensor> processDS = mapDS.process(new SplitProcessFun());
        processDS.print("main");
        processDS.getSideOutput(outtag).print("out");

        env.execute();
    }

    private static class SplitProcessFun extends ProcessFunction<WaterSensor, WaterSensor>
    {
        @Override
        public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
            Integer vc = waterSensor.getVc();
            if (vc > 30) {
                collector.collect(waterSensor);
            } else {
                context.output(outtag, new Tuple2(waterSensor.getId(), vc));
            }

        }
    }
}
