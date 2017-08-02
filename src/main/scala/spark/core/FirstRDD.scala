package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object FirstRDD {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("FirstRDD")
    val sc = new SparkContext(sparkConf)

    val inputNumbers = Array(20, -63, 11, 33, -10, 47, 0, 22, -3, 68, 93, 9, 28)
    val inputNumbersRDD = sc.parallelize(inputNumbers)

    val totalSum = inputNumbersRDD.reduce((a, b) => a + b)
    println(s"Total sum is $totalSum")

    val positiveNumbersRDD = inputNumbersRDD.filter(_ > 0)
    positiveNumbersRDD.foreach(println)

    val positiveNumbers = positiveNumbersRDD.collect()
    println(positiveNumbers.mkString(","))

    val largestNum = inputNumbersRDD.reduce((a, b) => if (a > b) a else b)
    println(s"Largest Number is $largestNum")

    sc.stop()
  }
}
