import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
object UniqueVisitor2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.readTextFile("D:\\MyWork\\ideaspace\\flink-project\\src\\main\\resources\\UserBehavior.csv")

    stream.map(data=>{
      val arr = data.split(",")
      UserBehavior(arr(0),arr(1),arr(2),arr(3),arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior =="pv")
      .map(data=>{
        ("dummykey",data.userId)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(60*60))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())
     // .print()

    env.execute()
  }
}
class MyTrigger() extends  Trigger[(String,String),TimeWindow]{
  override def onElement(element: (String, String),
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {

    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
    if (ctx.getCurrentWatermark >= window.getEnd) {
      val jedis = new Jedis("hadoop102", 6379)
      val key = window.getEnd.toString
      TriggerResult.FIRE_AND_PURGE
      println(key, jedis.hget("UvCountHashTable", key))
    }
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow,
                     ctx: Trigger.TriggerContext): Unit = {

  }
}

//自定义窗口处理函数
class UvCountWithBloom extends ProcessWindowFunction[(String,String),UvCount,String,TimeWindow]{

  lazy val jedis = new Jedis("hadoop102",6379)
  lazy  val bloom = new Bloom(1<<29)

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, String)],
                       out: Collector[UvCount]): Unit = {

    //key:窗口结束时间   value: 对应个数  存储在redis中
    val storeKey = context.window.getEnd.toString


    var count = 0L

    //hget(表,key) 获取value
    if(jedis.hget("UvcountHashTable",storeKey)!= null){
      count = jedis.hget("UvcountHashTable",storeKey).toLong
    }
    //实际每个elements中只有一个数值
    val userId = elements.last._2

    //对uesrId通过哈希进行计算
    val offset = bloom.hash(userId,61)

    //针对storekey 获取偏移量offset
    val isExist= jedis.getbit(storeKey,offset)

    //如果不存在 添加到表中
    if(!isExist){
      jedis.setbit(storeKey,offset,true)
      jedis.hset("UvcountHashTable",storeKey,(count + 1).toString)

     // out.collect(UvCount(storeKey.toLong,count +1 ))

    }

  }
}

//定义一个布隆过滤器
class Bloom(size:Long)  extends Serializable{
  private val cap = size

  def hash(value:String,seed:Int):Long={
    var result = 0
    // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
    for(i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    (cap -1) & result
  }

}