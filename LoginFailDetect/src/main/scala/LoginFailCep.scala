import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.Map
object LoginFailCep {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.fromElements[LoginEvent](
      LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
      LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
      LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
      LoginEvent("2", "192.168.10.10", "success", "1558430845"))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
    //设置pattern格式
    val pattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    val patternStream = CEP.pattern(stream.keyBy(_.userId),pattern)

    //import scala.collection.Map  否则报错
    val result = patternStream.select((pattern: Map[String, Iterable[LoginEvent]]) => {
      val first = pattern.getOrElse("begin", null).iterator.next()
      val second = pattern.getOrElse("next", null).iterator.next()
      (second.userId, second.ip, second.eventType)
    })
    result.print()
    env.execute()
  }
}
