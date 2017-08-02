package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object FirstApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("FirstApp")
    val sc = new SparkContext(sparkConf)

    val sampleFile = "data/sampleText.txt"
    val sampleTextFile = sc.textFile(sampleFile)
    val totalLines = sampleTextFile.count
    val numAs = sampleTextFile.filter(line => line.contains("a")).count
    val numBs = sampleTextFile.filter(line => line.contains("b")).count
    println(s"Lines with A's: $numAs, lines with B's: $numBs")
    println(s"Total lines: $totalLines")

    sc.stop()
  }
}
