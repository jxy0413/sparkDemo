package cn.bjfu.sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object createRdd_array {
  def main(args: Array[String]): Unit = {
    //创建sparkConf并设置App名称
    val conf = new SparkConf().setAppName("createRdd_array").setMaster("local[*]")
    //创建sparkContext对象，是其Spark App入口
    val sc = new SparkContext(conf)
    //使用parllelize()创建rdd
    val rdd = sc.parallelize(Array(1,2,3,4,5,6))
    rdd.foreach(println)
    println("分区数："+rdd.partitions.size)
    //******************************************
    //使用makeRDD()创建rdd
    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6,7,8))
    rdd1.foreach(println)
    println("分区数: "+rdd1.partitions.size)
    sc.stop()
  }
}
