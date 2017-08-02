package spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: Accumulator <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    lines.foreachRDD { rdd =>
      val errorsCounter = ErrorsCounter.getInstance(rdd.sparkContext)
      rdd.foreach(line =>
        if (line.contains("error")) {
          errorsCounter.add(1l)
        })
      println("Number of times error keyword occurred: " + errorsCounter.value)
    }
  }
}

object ErrorsCounter {

  @volatile private var instance: LongAccumulator = _

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("ErrorsCounter")
        }
      }
    }
    instance
  }
}
