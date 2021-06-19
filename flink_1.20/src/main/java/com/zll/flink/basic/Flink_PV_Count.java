package com.zll.flink.basic;

import com.zll.flink.bean.PageViewCount;
import com.zll.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

/**
 * @ClassName Flink_PV_Count
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-09 16:51
 * @Version 1.0
 */
public class Flink_PV_Count {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDS = env.readTextFile("flink_1.20/input/UserBehavior.csv");
        env.setStateBackend(new MemoryStateBackend());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<UserBehavior> filterDS = inputDS.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
            }
        }).filter(data -> "pv".equals(data.getBehavior()));

        WatermarkStrategy<UserBehavior> waterSensorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior , long l) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = filterDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("PV_" + new Random().nextInt(8), 1);
                    }
                });
        SingleOutputStreamOperator<PageViewCount> aggregateDS = mapDS.keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PageViewAggFunc(), new PageViewWindowFunc());
        aggregateDS.keyBy(PageViewCount::getTime)
                .process(new KeyedProcessFunction<String, PageViewCount, PageViewCount>() {
                    private ListState<PageViewCount> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("listState", PageViewCount.class));
                    }
                    @Override
                    public void processElement(PageViewCount pageViewCount, Context context, Collector<PageViewCount> collector) throws Exception {
                        listState.add(pageViewCount);
                        String time = pageViewCount.getTime();
//                        System.out.println(time);
//                        new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").format(new Date(time)).
//                                long ts = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse(time).getTime();
                                context.timerService().registerEventTimeTimer(Long.valueOf(time) +1);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
                        Iterable<PageViewCount> pageViewCounts = listState.get();
                        Integer count = 0;
                        Iterator<PageViewCount> iterator = pageViewCounts.iterator();
                        while (iterator.hasNext()) {
                            PageViewCount next = iterator.next();
                            count += next.getCount();
                        }
                        out.collect(new PageViewCount("PV", new Timestamp(timestamp - 1).toString(), count));
                        listState.clear();
                    }
                })
                .print();
        env.execute();
    }

    private static class PageViewAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }
        @Override
        public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
            return 1 + integer;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return integer + acc1;
        }
    }

    private static class PageViewWindowFunc implements WindowFunction<Integer, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<Integer> iterable, Collector<PageViewCount> collector) throws Exception {
            String timeStamp = String.valueOf(timeWindow.getStart());
            Integer count = iterable.iterator().next();
            collector.collect(new PageViewCount("PV", timeStamp, count));
        }
    }
}
