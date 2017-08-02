package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object PairRDDs {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("PairRDDs")
    val sc = new SparkContext(sparkConf)

    val sampleInput1 = sc.parallelize(List((4, 1), (6, 2), (4, 8), (3, 6), (9, 5), (3, 3)))
    val sampleInput2 = sc.parallelize(List((3, 1), (5, 9), (4, 8)))

    val groupByKey = sampleInput1.groupByKey()
    println("groupByKey")
    groupByKey.foreach(println)

    val reduceByKey = sampleInput1.reduceByKey((x, y) => x + y)
    println("reduceByKey")
    reduceByKey.foreach(println)

    val sortByKey = sampleInput1.sortByKey()
    println("sortByKey")
    sortByKey.foreach(println)

    val countByKey = sampleInput1.countByKey()
    println("countByKey")
    countByKey.foreach(println)

    val subtractByKey = sampleInput1.subtractByKey(sampleInput2)
    println("subtractByKey")
    subtractByKey.foreach(println)

    val mapValues = sampleInput1.mapValues(x => x + 1)
    println("mapValues")
    mapValues.foreach(println)

    val keys = sampleInput1.keys
    println("keys")
    keys.foreach(println)

    val values = sampleInput1.values
    println("values")
    values.foreach(println)

    val lookup = sampleInput1.lookup(4)
    println("lookup")
    lookup.foreach(println)

    val collectAsMap = sampleInput1.collectAsMap()
    println("collectAsMap")
    collectAsMap.foreach(println)

    sc.stop()
  }
}
