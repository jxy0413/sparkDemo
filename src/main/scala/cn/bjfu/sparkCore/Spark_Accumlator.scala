package cn.bjfu.sparkCore

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

//自定义累加器
//统计所有以H开头的单词 单词以及出现次数(word,count)
object Spark_Accumlator {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd  = sc.makeRDD(List("Hellp","spark","hello","Hello","Hello"))

//    val filterRdd = rdd.filter(_.substring(0,1).equals("H"))
//
//    val filterMapRdd = filterRdd.map((_,1))
//    filterMapRdd.reduceByKey(_+_).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }
}
//定义类 继承AccumulatorV2
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
  override def isZero: Boolean = ???

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = ???

  override def reset(): Unit = ???

   override def add(v: String): Unit = ???

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = ???

  override def value: mutable.Map[String, Int] = ???
}