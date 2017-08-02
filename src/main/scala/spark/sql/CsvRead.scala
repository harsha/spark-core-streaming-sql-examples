package spark.sql

import org.apache.spark.sql.SparkSession

object CsvRead {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("CsvRead")
      .getOrCreate()

    val sampleCsvFile = "data/sampleCsv.csv"
    val input = spark.read.option("header", value = true).csv(sampleCsvFile)

    input.createOrReplaceTempView("tweets")
    val results = spark.sql("SELECT user, location FROM tweets")
    results.collect.foreach(println)

    spark.stop()
  }
}
