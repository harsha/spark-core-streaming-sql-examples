package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object RDDOperations {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("RDDOperations")
    val sc = new SparkContext(sparkConf)

    val stringsRDD = sc.parallelize(List("hello", "world", "hello world", "hi", "Goodbye", "hello"))
    val greetingsRDD = sc.parallelize(List("hello", "hi", "howdy", "Good Morning"))

    val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))

    val uppercaseRDD = stringsRDD.map(x => x.toUpperCase)
    println("uppercaseRDD")
    uppercaseRDD.foreach(println)

    val flatMappedRDD = stringsRDD.flatMap(line => line.split(" "))
    println("flatMappedRDD")
    flatMappedRDD.foreach(println)

    val distinctRDD = stringsRDD.distinct
    println("distinctRDD")
    distinctRDD.foreach(println)

    val unionRDD = stringsRDD.union(greetingsRDD)
    println("unionRDD")
    unionRDD.foreach(println)

    val intersectionRDD = stringsRDD.intersection(greetingsRDD)
    println("intersectionRDD")
    intersectionRDD.foreach(println)

    val subtractRDD = stringsRDD.subtract(greetingsRDD)
    println("subtractRDD")
    subtractRDD.foreach(println)

    val countByValue = stringsRDD.countByValue()
    println(s"CountByValue :" + countByValue.mkString(","))

    val sumOfNumbers = numbersRDD.reduce((a, b) => a + b)
    println(s"Sum: $sumOfNumbers")

    val sumWithFold = numbersRDD.fold(0)((a, b) => a + b)
    println(s"SumWithFold: $sumWithFold")

    val agg = numbersRDD.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val avg = agg._1 / agg._2.toDouble
    println(s"Avg: $avg")

    val maxNum = numbersRDD.max
    println(s"Max: $maxNum")

    val minNum = numbersRDD.min
    println(s"Min: $minNum")

    val topFive = numbersRDD.top(5)
    println("Top Five: " + topFive.mkString(","))

    sc.stop()
  }
}
