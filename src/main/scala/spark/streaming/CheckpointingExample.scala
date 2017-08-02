package spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CheckpointingExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: CheckpointingExample <brokers> <topics> <checkpointDirectory>")
      System.exit(1)
    }

    val Array(brokers, topics, checkpointDirectory) = args

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(brokers, topics, checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(brokers: String, topics: String, checkpointDirectory: String): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("CheckpointingExample").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val words = lines.flatMap(line => line.split(" "))
    val output = words.map(word => (word, 1)).reduceByKey(_ + _)
    output.print()
    ssc.checkpoint(checkpointDirectory)
    ssc
  }
}
