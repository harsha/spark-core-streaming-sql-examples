package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TextFileStream {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Please pass text input directory path as argument. That is, any file system compatible with the HDFS API (HDFS, S3, NFS, etc.)")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("TextFileStream").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val input = ssc.textFileStream(args(0))
    val words = input.flatMap(line => line.split(" "))
    val output = words.map(word => (word, 1)).reduceByKey(_ + _)
    output.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
