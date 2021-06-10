package com.zll.flink.basic;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName Flink_PV_Wc20
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-08 17:16
 * @Version 1.0
 */
public class Flink_PV_Wc20 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> propertiesStream = env.socketTextStream("hadoop102", 7777);
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 8888);
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
        BroadcastStream<String> broadcast = propertiesStream.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<String, String> connect = dataStream.connect(broadcast);
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                String aSwitch = broadcastState.get("Switch");
                if ("1".equals(aSwitch)) {
                    collector.collect("读取了广播状态，切换1");
                } else if ("2".equals(aSwitch)) {
                    collector.collect("读取了广播状态，切换2");
                } else {
                    collector.collect("读取了广播状态，切换到其他");
                }
            }
            @Override
            public void processBroadcastElement(String s, Context context, Collector<String> collector) throws Exception {
                BroadcastState<String, String> broadcastState = context.getBroadcastState(mapStateDescriptor);
                broadcastState.put("Switch", s);
            }
        }).print();
        env.execute();
    }
}
