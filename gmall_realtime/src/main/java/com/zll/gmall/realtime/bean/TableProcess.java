package com.zll.gmall.realtime.bean;

/**
 * @ClassName TableProcess
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-01 9:55
 * @Version 1.0
 */
public class TableProcess {
//    动态分流sink常量
    public static final String SINK_TYPE_HBASE = "HBASE";
    public static final String SINK_TYPE_KAFKA = "KAFKA";
    public static final String SINK_TYPE_CK = "CLICKHOUSE";
//    来源表
    String sourceTable;
//    操作类型 insert，update，delete
    String operateType;
//    输出类型 hbase kafka
    String sinkType;
//    输出表(主题)
    String sinkTable;
//    输出字段
    String sinkColumns;
//    主键字段
    String sinkPk;
//    建表扩展
    String sinkExtend;
}
