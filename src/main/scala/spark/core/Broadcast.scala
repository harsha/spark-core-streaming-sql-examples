package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Broadcast")
    val sc = new SparkContext(sparkConf)

    val broadcast = sc.broadcast(Array(1, 2, 3))

    println("Value From Broadcast: " + broadcast.value.mkString(","))
    sc.stop()
  }
}
