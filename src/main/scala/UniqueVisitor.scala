import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("D:\\MyWork\\ideaspace\\flink-project\\src\\main\\resources\\UserBehavior.csv")
    dataStream.map(line => {
      val arr = line.split(",")
      UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy(_.behavior)
      .timeWindow(Time.seconds(60 * 60))
      .process(new UvCountByWindow)
      .print()

    env.execute()
  }
}
//    dataStream.map(line=>{
//      val arr = line.split(",")
//      UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
//    }).assignAscendingTimestamps(_.timestamp * 1000)
//      .filter(_.behavior == "pv")
//      .timeWindowAll(Time.seconds(60*60))
//      .process(new UvCountByWindow)
//
//  }
//}

class UvCountByWindow extends ProcessWindowFunction[UserBehavior, UvCount, String, TimeWindow] {

  override def process(key: String,
                       context: Context,
                       elements: Iterable[UserBehavior],
                       out: Collector[UvCount]): Unit = {
    //需要制定Set内的类型对其进行初始化
    var s: Set[String] = Set()
    for (user <- elements.iterator) {
      s += user.userId
    }
    out.collect(UvCount(context.window.getEnd, s.size))
  }
}

//class UvCountByWindow extends ProcessAllWindowFunction[UserBehavior,UvCount,TimeWindow]{
//  override def process(context: Context,
//                       elements: Iterable[UserBehavior],
//                       out: Collector[UvCount]): Unit = {
//    var s: Set[String] = Set()
//    for (user <- elements.iterator) {
//      s += user.userId
//    }
//    out.collect(UvCount(context.window.getEnd, s.size))
//  }
//}