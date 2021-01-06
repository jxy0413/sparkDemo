package cn.bjfu.sparkCore

import org.apache.spark.rdd.RDD

object value01_map {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4,2)

    val mapRdd: RDD[Int] = rdd.map(_*2)

    mapRdd.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
