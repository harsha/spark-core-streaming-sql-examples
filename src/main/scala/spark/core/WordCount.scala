package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val sampleFile = "data/sampleText.txt"
    val input = sc.textFile(sampleFile)
    val words = input.flatMap(line => line.split(" "))
    val output = words.map(word => (word, 1)).reduceByKey(_ + _).sortBy(x => x._2, ascending = false)
    output.foreach(record => println(record))

    sc.stop()
  }
}
