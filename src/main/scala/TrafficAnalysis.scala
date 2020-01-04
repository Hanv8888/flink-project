
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object TrafficAnalysis {
  def main(args: Array[String]): Unit = {

  }
}
