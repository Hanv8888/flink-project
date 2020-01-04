
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map
case class OrderEvent(orderId: String, eventType: String, eventTime: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //确保水位线正常插入，可以使用socket作为数据源
    val stream = env.fromElements(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("2", "pay", "1558430844"),
      OrderEvent("3", "pay", "1558430942"),
      OrderEvent("1", "pay", "1558430943"))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)

    //设置匹配模式
    val pattern = Pattern.begin[OrderEvent]("begin").where(_.eventType.equals("create"))
      .next("next").where(_.eventType.equals("pay")).within(Time.seconds(5))

    //匹配数据流
    val patternStream = CEP.pattern(stream.keyBy(_.orderId),pattern)

    //侧输出流标签
    val orderTimeoutOutput  = new OutputTag[OrderEvent]("sideoutput")

    //    val selectStream = patternStream.select((pattern: Map[String, Iterable[OrderEvent]]) => {
    //      val first = pattern.getOrElse("begin", null).iterator.next()
    //      val second = pattern.getOrElse("next", null).iterator.next()
    //      (second.orderId, second.eventType, second.eventTime)
    //    })
    //    selectStream.print()

    //指定超时事件函数
    val timeoutFunction = (map:Map[String,Iterable[OrderEvent]],timestamp:Long,out:Collector[OrderEvent])=>{
      println(timestamp)
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    }
    //指定未超时事件函数
    val selectFunction = (map:Map[String,Iterable[OrderEvent]],out:Collector[OrderEvent])=>{

    }
    //获取超时时间方式：flatSelect(侧输出标签)(超时事件函数)(未超时事件函数)
    val timeoutOrder = patternStream.flatSelect(orderTimeoutOutput)(timeoutFunction)(selectFunction)

    //获取侧输出流中超时事件
    timeoutOrder.getSideOutput(orderTimeoutOutput).print()


    //    def select[L: TypeInformation, R: TypeInformation](outputTag: OutputTag[L])(
    //      patternTimeoutFunction: (Map[String, Iterable[T]], Long) => L) (
    //      patternSelectFunction: Map[String, Iterable[T]] => R)
    //    : DataStream[R] = {


    //    def flatSelect[L: TypeInformation, R: TypeInformation](outputTag: OutputTag[L])(
    //      patternFlatTimeoutFunction: (Map[String, Iterable[T]], Long, Collector[L]) => Unit) (
    //      patternFlatSelectFunction: (Map[String, Iterable[T]], Collector[R]) => Unit)
    //    : DataStream[R] = {


    env.execute()
  }
}
