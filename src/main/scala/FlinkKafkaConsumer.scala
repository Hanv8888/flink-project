import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object FlinkKafkaConsumer {
  def main(args: Array[String]): Unit = {
      val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties)).print()
    env.execute()


  }
}
