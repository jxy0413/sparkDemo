package cn.bjfu.sparkCore

import org.apache.spark.rdd.RDD

object KeyValue09_join {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建两个pairRDD，并将key相同的数据聚合到一个元组
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))
    rdd.join(rdd1).collect().foreach(println)

    rdd.cogroup(rdd1).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
