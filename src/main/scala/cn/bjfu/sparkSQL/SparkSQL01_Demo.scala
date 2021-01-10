package cn.bjfu.sparkSQL

import org.apache.spark.sql.SparkSession

object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建配置对象
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val frame = spark.read.json("C:\\Users\\Administrator\\IdeaProjects\\sparkDemo\\input\\3.txt")
    frame.show()
    //4.关闭连接
    spark.stop()
  }
}
