package com.zll.flink.exer;

import com.zll.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * @ClassName Flink_PV_Wc4
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 12:30
 * @Version 1.0
 */
public class Flink_PV_Wc4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDS = env.addSource(new SourceFunction<MarketingUserBehavior>() {
            boolean canRun = true;
            Random random = new Random();
            List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
            List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

            @Override
            public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );
                sourceContext.collect(marketingUserBehavior);
                Thread.sleep(200);
            }
            @Override
            public void cancel() {
                canRun = false;
            }
        });

        KeyedStream<MarketingUserBehavior, Tuple2<String, String>> marketingUserKeyedStream = marketingUserBehaviorDS.keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(MarketingUserBehavior marketingUserBehavior) throws Exception {
                return new Tuple2<>(marketingUserBehavior.getChannel(), marketingUserBehavior.getBehavior());
            }
        });
        marketingUserKeyedStream.process(new KeyedProcessFunction<Tuple2<String,String>, MarketingUserBehavior, Tuple2<Tuple2<String,String>,Integer>>() {
            private HashMap<String, Integer> fk =  new HashMap();
            @Override
            public void processElement(MarketingUserBehavior marketingUserBehavior, Context context, Collector<Tuple2<Tuple2<String, String>, Integer>> collector) throws Exception {
                String key = marketingUserBehavior.getChannel() + "-" + marketingUserBehavior.getBehavior();
                Integer count = fk.getOrDefault(key, 0);
                count++;
                collector.collect(new Tuple2<>(context.getCurrentKey(), count));
                fk.put(key, count);
            }
        }).print();
        env.execute();
    }
}
