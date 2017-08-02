package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStream {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Please pass hostname and port as arguments")
      System.exit(1)
    }

    val hostName = args(0)
    val port = args(1).toInt

    val sparkConf = new SparkConf().setAppName("SocketStream").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val input = ssc.socketTextStream(hostName, port, StorageLevel.MEMORY_ONLY)
    val words = input.flatMap(line => line.split(" "))
    val output = words.map(word => (word, 1)).reduceByKey(_ + _)
    output.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
