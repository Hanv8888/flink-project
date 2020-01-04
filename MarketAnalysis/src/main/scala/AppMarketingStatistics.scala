import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new SimulatedEventSource)
    stream.assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data=>{
        ("dummyKey",1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(60),Time.seconds(1))
      .process(new MarketingCountTotal())
      .print()

    env.execute()

  }
}
class MarketingCountTotal extends ProcessWindowFunction[(String,Long),MarketingCountView,String,TimeWindow]{
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[MarketingCountView]): Unit = {
    val windowStart= context.window.getStart
    val windowEnd = context.window.getEnd
    val count = elements.size

    out.collect(MarketingCountView(formatTs(windowStart),formatTs(windowEnd),"Total","Total",count))
  }
  private def formatTs(ts:Long) = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(new Date(ts))
  }
}