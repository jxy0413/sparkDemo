package cn.bjfu.sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object createRdd_file {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[*]").setAppName("createRdd_file")
     val sc = new SparkContext(conf)
     val lineWordRdd = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\sparkDemo\\input\\*.txt")
     lineWordRdd.foreach(println)
     sc.stop()
  }
}
