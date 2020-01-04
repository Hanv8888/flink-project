import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.fromElements(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("2", "pay", "1558430844"),
      OrderEvent("3", "pay", "1558430942"),
      OrderEvent("1", "pay", "1558430943"))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)

    stream.keyBy(_.orderId).process(new OrderMatchFunction).print()

    env.execute()

  }
}
class OrderMatchFunction  extends KeyedProcessFunction[String,OrderEvent,OrderEvent]{

  lazy val orderState: ValueState[OrderEvent] = getRuntimeContext
                  .getState(new ValueStateDescriptor[OrderEvent]("saved order",classOf[OrderEvent]))

  override def processElement(value: OrderEvent,
                              ctx: KeyedProcessFunction[String, OrderEvent, OrderEvent]#Context,
                              out: Collector[OrderEvent]): Unit = {
    if(value.eventType == "create"){
      if(orderState.value() == null){
        orderState.update(value)
      }
    }else {
      orderState.update(value)
    }
    ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000  + 5 * 1000)

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, OrderEvent, OrderEvent]#OnTimerContext,
                       out: Collector[OrderEvent]): Unit = {
    val savedOrder = orderState.value()

    if(savedOrder != null && (savedOrder.eventType == "create")){
      out.collect(savedOrder)
    }
    orderState.clear()
  }
}