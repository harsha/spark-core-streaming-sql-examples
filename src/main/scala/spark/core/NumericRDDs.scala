package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object NumericRDDs {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("NumericRDDs")
    val sc = new SparkContext(sparkConf)

    val numbersRDD = sc.parallelize(1 to 100)

    val stats = numbersRDD.stats()

    println("Count: " + stats.count)

    println("Mean: " + stats.mean)

    println("Sum: " + stats.sum)

    println("Max: " + stats.max)

    println("Min: " + stats.min)

    println("Variance: " + stats.variance)

    println("Sample Variance: " + stats.sampleVariance)

    println("Stdev: " + stats.stdev)

    println("Sample Stdev: " + stats.sampleStdev)

    sc.stop()
  }
}
