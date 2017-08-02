package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Accumulator")
    val sc = new SparkContext(sparkConf)

    val accum = sc.longAccumulator("My Accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))

    println("Accumulator Value: " + accum.value)
    sc.stop()
  }
}
