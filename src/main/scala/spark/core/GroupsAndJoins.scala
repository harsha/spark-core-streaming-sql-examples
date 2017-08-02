package spark.core

import org.apache.spark.{SparkConf, SparkContext}

object GroupsAndJoins {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("GroupsAndJoins")
    val sc = new SparkContext(sparkConf)

    val sampleInput1 = sc.parallelize(List((4, 1), (6, 2), (4, 8), (3, 6), (9, 5), (3, 3)))
    val sampleInput2 = sc.parallelize(List((3, 1), (5, 9), (4, 8)))

    val outputJoin = sampleInput1.join(sampleInput2)
    println("Inner Join: " + outputJoin.collect().mkString(","))

    val outputLeftOuterJoin = sampleInput1.leftOuterJoin(sampleInput2)
    println("Left Outer Join: " + outputLeftOuterJoin.collect().mkString(","))

    val outputRightOuterJoin = sampleInput1.rightOuterJoin(sampleInput2)
    println("Right Outer Join: " + outputRightOuterJoin.collect().mkString(","))

    val coGroup = sampleInput1.cogroup(sampleInput2)
    println("CoGroup: " + coGroup.collect().mkString(","))

    sc.stop()
  }
}
