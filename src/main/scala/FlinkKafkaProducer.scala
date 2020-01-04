import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object FlinkKafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic:String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
   // val resourcePath = getClass.getResource("D:\\MyWork\\ideaspace\\flink-project\\src\\main\\resources\\UserBehavior.csv")
    val bufferedSource = io.Source.fromFile("D:\\MyWork\\ideaspace\\flink-project\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()
  }
}
