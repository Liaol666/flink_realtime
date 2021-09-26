import java.sql
import java.sql.DriverManager
import java.util.Properties

import com.mysql.jdbc.{Connection, PreparedStatement}
import org.apache.flink.api.common.functions.{FilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    var host = params.get("host");
//    val port = params.getInt("port");
    val port = params.getInt("port");
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDstrem: DataStream[String] = env.socketTextStream(host, port)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop102:9092")
    prop.setProperty("group.id", "consumer-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")
    val kafkaDs = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(),prop))

    textDstrem.flatMap(new myFlatMap())

    env.addSource(new MySenSourceFunction())

    val dataStraem: DataStream[(String, Int)] = textDstrem.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)

    dataStraem.addSink(new myJDBCSink())

    dataStraem.print().setParallelism(1)

    dataStraem.filter(new keyedFilter(""))

    env.execute("StreamWordCount")
    

  }

  case class MySenSourceFunction() extends SourceFunction[String] {
    var running = true
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    }

    override def cancel(): Unit =
      {
        running = false
      }
  }

  case class keyedFilter(name:String) extends FilterFunction[(String, Int)] {
    override def filter(value: (String, Int)): Boolean = ???
  }

  case class myFlatMap() extends RichFlatMapFunction[String,String] {
    var subtask = 0
    override def open(parameters: Configuration): Unit = {
      subtask = getRuntimeContext.getIndexOfThisSubtask
    }
    override def flatMap(value: String, out: Collector[String]): Unit = {
      out.collect("")
    }

    override def close(): Unit = {

    }
  }

  case class myJDBCSink() extends RichSinkFunction[(String,Int)] {
    var conn:sql.Connection = _
    var insertStmt:PreparedStatement = _
    var updateStmt:PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3361/test", "root", "root")
      conn.prepareStatement("insert into tem(name,age) values (?, ?)")
    }

    override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {

    }


  }

}
