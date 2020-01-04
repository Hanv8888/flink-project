import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector




case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)
object LoginFail {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.fromElements[LoginEvent](
      LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
      LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
      LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
      LoginEvent("2", "192.168.10.10", "success", "1558430845"))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userId)
      .process(new MatchFunction())
      .print()

    env.execute()
  }
}
class MatchFunction extends KeyedProcessFunction[String,LoginEvent,String]{
  //定义存储登录失败日志的集合状态
  lazy  val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved login",classOf[LoginEvent]))

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[String, LoginEvent, String]#Context,
                              out: Collector[String]): Unit = {
    //方案一：将第一次失败的添加到集合状态中，并注册2s的定时器，如果回调函数中状态内集合的长度>2 则输出
    //      //如果失败  则添加到状态中
    //     if(value.eventType == "fail"){
    //       loginState.add(value)
    //     }
    //    //注册定时器，触发时间设定为2秒后
    //    ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 2 * 1000 )
    //  }


    //  override def onTimer(timestamp: Long,
    //                       ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext,
    //                       out: Collector[String]): Unit = {
    //     val allLogins:ListBuffer[LoginEvent] = ListBuffer()
    //    import scala.collection.JavaConversions._
    //    for(login <- loginState.get){
    //       allLogins += login
    //    }
    //    loginState.clear()
    //
    //    if(allLogins.size > 1 ){
    //      out.collect("2s内连续登录失败")
    //    }
    //  }

    //改进：
    //首先按照type做筛选，如果success直接清空，如果fail再做处理
    if(value.eventType == "fail"){
      //如果已经有登录失败的数据，那么判断是否在两秒内
       val iter = loginState.get().iterator()
      if(iter.hasNext){
        val firstFail = iter.next()
        //如果两次登录失败时间间隔小于2秒，输出报警
        if(value.eventTime.toLong < firstFail.eventTime.toLong + 2L ){
          out.collect(value.userId + "2s内连续登录失败")
        }
        //把最近一次的登录失败数据更新到state中
        val failList = new util.ArrayList[LoginEvent]()
        failList.add(value)
        loginState.update(failList)
      }else{
        //如果state中没有登录失败的数据，则直接添加到state中
        loginState.add(value)
      }
    }else {
      loginState.clear()
    }
  }
}