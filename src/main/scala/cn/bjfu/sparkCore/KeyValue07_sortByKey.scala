package cn.bjfu.sparkCore

import org.apache.spark.rdd.RDD

object KeyValue07_sortByKey {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    //3.按照key的正序排序
    rdd.sortByKey(true).collect().foreach(println)
    rdd.sortByKey(false).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
