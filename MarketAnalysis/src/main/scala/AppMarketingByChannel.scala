import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
case class MarketingCountView(windowStart: String,
                              windowEnd: String,
                              channel: String,
                              behavior: String,
                              count: Long)
  object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.setParallelism(1)

      val stream = env.addSource(new SimulatedEventSource)
      stream.assignAscendingTimestamps(_.timestamp)
        .filter(_.behavior != "UNINSTALL")
        .map(data=>{
          ((data.channel,data.behavior),1L)
        })
        .keyBy(_._1)
        .timeWindow(Time.seconds(60),Time.seconds(1))
        .process(new MarketingCountByChannel())
        .print()

      env.execute()


    }

}
case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timestamp:Long)

class SimulatedEventSource extends  RichParallelSourceFunction[MarketingUserBehavior]{

  var running = true
  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand: Random = Random


  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
      var maxElements = Long.MaxValue
      var count = 0L

    while (running && count < maxElements){
      val id = UUID.randomUUID().toString
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collectWithTimestamp(MarketingUserBehavior(id,behaviorType,channel,ts),ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }

  override def cancel(): Unit = {
          running = false
  }
}

class MarketingCountByChannel   extends  ProcessWindowFunction[((String,String),Long),MarketingCountView,(String,String),TimeWindow]{
  override def process(key: (String, String),
                       context: Context,
                       elements: Iterable[((String, String), Long)],
                       out: Collector[MarketingCountView]): Unit = {
    val windowStart= context.window.getStart
    val windowEnd = context.window.getEnd
    val channel  = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketingCountView(formatTs(windowStart),formatTs(windowEnd),channel,behavior,count))
  }

  private def formatTs(ts:Long) = {
     val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(new Date(ts))
  }
}