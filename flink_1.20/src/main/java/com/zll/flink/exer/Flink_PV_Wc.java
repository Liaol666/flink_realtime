package com.zll.flink.exer;

import com.zll.flink.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Flink_PV_Wc
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-03 10:16
 * @Version 1.0
 */
public class Flink_PV_Wc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("flink_1.20/input/UserBehavior.csv")
                .map(line-> {
                    String[] strings = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(strings[0]),
                            Long.valueOf(strings[1]),
                            Integer.valueOf(strings[2]),
                            strings[3],
                            Long.valueOf(strings[4])
                    );
                }).filter(behavior-> "pv".equals(behavior.getBehavior()))
                .map(behavior->Tuple2.of("pv", 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1)
                .print();

        env.execute("Flink_PV_Wc");
    }
}
