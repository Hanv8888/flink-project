import java.sql.Timestamp


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class UserBehavior1(userId:String,
                         itemId:String,
                         categoryId:String,
                         behavior:String,
                         ts:Long)
case class ItemViewCount(itemId:String,
                         windowEnd:Long,
                         count:Long
                        )
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val Datastream = env.readTextFile("D:\\MyWork\\ideaspace\\flink-project\\src\\main\\resources\\UserBehavior.csv")


    Datastream.map(line=>{
      val arr = line.split(",")
      UserBehavior1(arr(0),arr(1),arr(2),arr(3),arr(4).toLong * 1000)
    }).assignAscendingTimestamps(_.ts)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new Agg,new Win)
      .keyBy("windowEnd")
      .process(new MyKey(3))
      .print()

    env.execute()
  }

}
class Agg extends AggregateFunction[UserBehavior1,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior1, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//当 .keyBy("itemId")使用字符串做key时，后面key的类型使用Tuple  获取值为Tuple1[类型].f0
class Win extends  ProcessWindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def process(key: Tuple,
                       context: Context,
                       elements: Iterable[Long],
                       out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[String]].f0
    out.collect(ItemViewCount(itemId,context.window.getEnd,elements.iterator.next()))

  }
}

class MyKey(topSize: Int) extends  KeyedProcessFunction[Tuple,ItemViewCount,String]{
  lazy val itemState: ListState[ItemViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
  )

  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    itemState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1 )
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allItems:ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get){
      allItems += item
    }

    itemState.clear()

    val sortedItems = allItems.sortBy(-_.count).take(topSize)
    val result = new StringBuilder
    result.append("===========================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1 )).append("\n")
    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No")
        .append(i +1)
        .append(":")
        .append(" 商品ID=")
        .append(currentItem.itemId)
        .append(" 浏览量=")
        .append(currentItem.count)
        .append("\n")
    }
    result.append("============================================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}
//
//时间：2017-11-26 09:15:00.0
//No1: 商品ID=812879 浏览量=7
//No2: 商品ID=138964 浏览量=5
//No3: 商品ID=4568476 浏览量=5