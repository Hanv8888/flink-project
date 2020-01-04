import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class PayEvent(orderId: String, eventType: String, eventTime: String)


object TxMatch {


  lazy val unmatchedOrders: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatchedOrders"){}
  lazy  val unmatchedPays: OutputTag[PayEvent] = new OutputTag[PayEvent]("unmatchedPays"){}

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orders = env.fromElements(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("1", "pay", "1558430844"),
      OrderEvent("2", "pay", "1558430845"),
      OrderEvent("3", "create", "1558430849"),
      OrderEvent("3", "pay", "1558430849"))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)


    val pay = env.fromElements(
      PayEvent("1", "weixin", "1558430847"),
      PayEvent("2", "zhifubao", "1558430848"),
      PayEvent("4", "zhifubao", "1558430850"))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)

    val processed = orders.connect(pay).process(new CoStreamFunction)

    processed.getSideOutput[PayEvent](unmatchedPays).print()
    processed.getSideOutput[OrderEvent](unmatchedOrders).print()

    //OrderEvent(3,pay,1558430849)
    //PayEvent(4,zhifubao,1558430850)

    env.execute()
  }

  class CoStreamFunction extends  CoProcessFunction[OrderEvent,PayEvent,(OrderEvent,PayEvent)]{

  lazy val orderState: ValueState[OrderEvent] = getRuntimeContext
                    .getState(new ValueStateDescriptor[OrderEvent]("saved order",classOf[OrderEvent]))

  lazy val payState: ValueState[PayEvent] = getRuntimeContext
                    .getState(new ValueStateDescriptor[PayEvent]("saved pay",classOf[PayEvent]))

  override def processElement1(order: OrderEvent,
                               ctx: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context,
                               out: Collector[(OrderEvent, PayEvent)]): Unit = {
    val pay = payState.value()
    if(pay != null){
      payState.clear()
      out.collect((order,pay))
    }else {
      orderState.update(order)
      ctx.timerService().registerEventTimeTimer(order.eventTime.toLong * 1000)
    }
  }

  override def processElement2(pay: PayEvent,
                               ctx: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context,
                               out: Collector[(OrderEvent, PayEvent)]): Unit = {
        val order = orderState.value()
    if(order != null){
      orderState.clear()
      out.collect((order,pay))
    }else {
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer(pay.eventTime.toLong * 1000)
    }
  }


  override def onTimer(timestamp: Long,
                       ctx: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#OnTimerContext,
                       out: Collector[(OrderEvent, PayEvent)]): Unit = {
         if(payState.value != null ){
           ctx.output(unmatchedPays,payState.value())
           payState.clear()
         }

          if(orderState.value != null){
            ctx.output(unmatchedOrders,orderState.value())
            orderState.clear()
          }
     }

  }
}