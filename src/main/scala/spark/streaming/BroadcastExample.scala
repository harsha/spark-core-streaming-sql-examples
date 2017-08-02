package spark.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage BroadcastExample <hostName> <port>")
      System.exit(1)
    }

    val hostName = args(0)
    val port = args(1).toInt

    val sparkConf = new SparkConf().setAppName("BroadcastExample").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val input = ssc.socketTextStream(hostName, port, StorageLevel.MEMORY_ONLY)
    val words = input.flatMap(line => line.split(" "))
    words.foreachRDD { rdd =>
      val keyWordsList = KeyWordsList.getInstance(rdd.sparkContext)
      rdd.foreach(word =>
        if (keyWordsList.value.contains(word)) {
          println("Keyword found: " + word)
        }
      )
    }
  }
}

object KeyWordsList {
  @volatile private var instance: Broadcast[Seq[String]] = _

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val keyWordsList = Seq("iPhone", "iPad", "iWatch", "iMac")
          instance = sc.broadcast(keyWordsList)
        }
      }
    }
    instance
  }
}