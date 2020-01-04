import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

object AdStatisticsByGeo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("D:\\MyWork\\ideaspace\\flink-project\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")
    stream.map(data=>{
      val arr = data.split(",")
      AdClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)
        .keyBy(_.province)
        .timeWindow(Time.seconds(60 * 60 ),Time.seconds(5))
        .aggregate(new Aggg ,new Pro)
        .print()


    env.execute()
  }
}
class Aggg extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class Pro extends ProcessWindowFunction[Long,CountByProvince,String,TimeWindow]{

  override def process(key: String,
                       context: Context,
                       elements: Iterable[Long],
                       out: Collector[CountByProvince]): Unit = {
    val end = context.window.getEnd
    out.collect(CountByProvince(formatTs(end),key,elements.iterator.next()))
  }
  private def formatTs (ts: Long) = {
    val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
    df.format (new Date (ts) )
  }

}

