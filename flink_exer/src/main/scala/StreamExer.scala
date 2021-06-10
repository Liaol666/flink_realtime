import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamExer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream: DataStream[String] = env.socketTextStream("", 7777)

    val textDS: DataStream[(String, Long, Int)] = dataStream.map(text => {
      val strings = text.split(" ")
      (strings(0), strings(1).toLong, 1)
    })
    val textEventDstream = textDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
      override def extractTimestamp(element: (String, Long, Int)): Long = {
        element._2
      }
    })
    val envenDstream: KeyedStream[(String, Long, Int), Tuple] = textEventDstream.keyBy(0)
    envenDstream.window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
    envenDstream.window(TumblingEventTimeWindows.of(Time.seconds(3)))


  }


}
