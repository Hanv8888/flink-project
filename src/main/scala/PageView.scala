import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("D:\\MyWork\\ideaspace\\flink-project\\src\\main\\resources\\UserBehavior.csv")
    dataStream.map(data=>{
      val arr = data.split(",")
      UserBehavior(arr(0),arr(1),arr(2),arr(3),arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(x=>("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60*60))
      .sum(1)
      .print()

    env.execute()

  }

}
case class UserBehavior(userId: String, itemId: String, categoryId: String, behavior: String, timestamp: Long)