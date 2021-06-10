import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount1 {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dsStream = env.socketTextStream(host, port)

    val dataStream = dsStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    dataStream.print().setParallelism(1)
    env.execute("StreamWordCount1")

  }

}
