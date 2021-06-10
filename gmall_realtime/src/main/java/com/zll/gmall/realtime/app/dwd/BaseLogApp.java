package com.zll.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zll.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @ClassName BaseLogApp
 * @Description TODO
 * @Author 17588
 * @Date 2021-05-18 18:05
 * @Version 1.0
 */
public class BaseLogApp {
    private static final String TOPIC_START ="dwd_start_log";
    private static final String TOPIC_PAGE ="dwd_page_log";
    private static final String TOPIC_DISPLAY ="dwd_display_log";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//设置并行度，与Kafka分区数保持一致
        env.setParallelism(1);
//设置CK相关的参数
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/dwd_log_ck"));

        System.setProperty("HADOOP_USER_NAME", "zll");

//        指定消费者配置信息
        String groupId = "dwd_log";
        String topic = "ods_base_log";

//        从kafka中读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

//       转换为JSONobject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(data -> JSONObject.parseObject(data));
//按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
//        使用状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream.map(new NewMidRichMapFunc());

//        jsonWithNewFlagDS.print();
// 分流，使用processfunction将ODS数据拆分成启动、曝光及页面数据
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new SplitProcessFuc());

        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start"){});
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display"){});

//        pageDS.print("page>>>>>");
//        startDS.print("start>>>>>>>");
//        displayDS.print("display>>>>>>>>>");
startDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_START));
displayDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY));
pageDS.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE));

//        执行
        env.execute("dwd_base_log Job");
    }

    public static class NewMidRichMapFunc extends RichMapFunction<JSONObject, JSONObject> {

//        声明状态用于表示当前Mid是否已经访问过
        private ValueState<String> firstVisitDateState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            String isNew = value.getJSONObject("common").getString("is_new");

//            如果当前前端传输数据表示为新用户，则进行校验
            if ("1".equals(isNew)) {
//                取出状态数据并取出当前访问用户
                String firstDate = firstVisitDateState.value();
                Long ts = value.getLong("ts");
                if (firstDate != null) {
//                    修复
                    value.getJSONObject("common").put("is_new", "0");
                } else {
                    firstVisitDateState.update(simpleDateFormat.format(ts));
                }
            }
            return value;
        }
    }
    public static class SplitProcessFuc extends ProcessFunction<JSONObject, String> {


        @Override
        public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
//            提取“start”字段
            String start = jsonObject.getString("start");
//            判断是否为启动数据
            if (start != null && start.length() > 0 ){
                context.output(new OutputTag<String>("start"){}, jsonObject.toString());
            }else {
//                继续判断是否为曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() >0 ) {
                    for (int i = 0; i < displays.size(); i++) {
//                        取出单条曝光
                        JSONObject displayJson = displays.getJSONObject(i);
//添加页面id
                        displayJson.put("page_id", jsonObject.getJSONObject("page").getString("page_id"));
                        context.output(new OutputTag<String>("display"){}, displayJson.toString());
                    }
                } else {
//                    为页面数据，将数据输出到主流
                    collector.collect(jsonObject.toString());
                }
            }
        }
    }

}
