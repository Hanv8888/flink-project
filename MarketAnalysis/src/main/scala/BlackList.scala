
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class BlackListWarning(userId: Long, adId: Long, msg: String)

object BlackList {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.readTextFile("D:\\MyWork\\ideaspace\\flink-project\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv")

    val filterStream = stream.map(data => {
      val arr = data.split(",")
      AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong * 1000)
    }).assignAscendingTimestamps(_.timestamp)
      .keyBy(data => {
        (data.userId, data.adId)
      }).process(new FilterBlackList(100))

    filterStream
      .keyBy(_.province)
      .timeWindow(Time.minutes(60), Time.seconds(5))
      .aggregate(new countAggg, new countResult)


    filterStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print()

    env.execute()


  }
}

class FilterBlackList(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  //定义侧输出流
  lazy val blackListOutputTag = new OutputTag[BlackListWarning]("blacklist")

  //保存当前用户对当前广告的点击量
  lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
  //标记当前（用户，广告）作为key是否第一次发送到黑名单
  lazy val firstSent = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("first-state", classOf[Boolean]))


  override def processElement(value: AdClickLog,
                              ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context,
                              out: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()
    //如果是第一次处理，注册一个定时器，每天00:00触发清除
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
      ctx.timerService().registerProcessingTimeTimer(ts)

    }
    //如果计数已经超过上限，则加入黑名单，用侧输出流输出警报信息

    if (curCount > maxCount) {
      if (!firstSent.value()) {
        firstSent.update(true)
        ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
      }
     return
    }
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext,
                       out: Collector[AdClickLog]): Unit = {
      firstSent.clear()
      countState.clear()
  }


}

class countAggg extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class countResult extends ProcessWindowFunction[Long, CountByProvince, String, TimeWindow] {

  override def process(key: String,
                       context: Context,
                       elements: Iterable[Long],
                       out: Collector[CountByProvince]): Unit = {
    val end = context.window.getEnd
    out.collect(CountByProvince(formatTs(end), key, elements.iterator.next()))
  }

  private def formatTs(ts: Long) = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(new Date(ts))
  }

}