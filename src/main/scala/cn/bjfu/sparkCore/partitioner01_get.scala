package cn.bjfu.sparkCore

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object partitioner01_get {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val pairRdd = sc.makeRDD(List((1,1),(2,2),(3,3)))

    //打印分区器
    println(pairRdd.partitioner)

    val partitionRDD: RDD[(Int, Int)] = pairRdd.partitionBy(new HashPartitioner(2))

    //3.3 打印分区器
    println(partitionRDD.partitioner)
    //4.关闭连接
    sc.stop()
  }
}