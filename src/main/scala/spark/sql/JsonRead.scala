package spark.sql

import org.apache.spark.sql.SparkSession

object JsonRead {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("JsonRead")
      .getOrCreate()

    val sampleJsonFile = "data/sampleJson.json"
    val input = spark.read.json(sampleJsonFile)

    input.createOrReplaceTempView("tweets")
    val results = spark.sql("SELECT user.name, text FROM tweets")
    results.collect.foreach(println)

    spark.stop()
  }
}
