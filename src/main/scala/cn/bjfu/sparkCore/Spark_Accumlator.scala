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

    //使用累加器
    val myAcc = new MyAccumulator;
    //注册累加器
    sc.register(myAcc)

    rdd.foreach{
      word=>{
        myAcc.add(word)
      }
    }
    //输出累加器结果
    println(myAcc.value)
    //4.关闭连接
    sc.stop()
  }
}
//定义类 继承AccumulatorV2
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
  //定义一个集合，集合单词以及出现次数
  var map = mutable.Map[String,Int]()

  //是否为初始状态
  override def isZero: Boolean = map.isEmpty

  //创建一个新的累加器对象
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val newAcc = new MyAccumulator
    newAcc.map  = this.map
    newAcc
  }

  //重置
  override def reset(): Unit = map.clear()

  //向累加器中添加元素
  override def add(elem: String): Unit = {
      if(elem.startsWith("H")){
          //向可变集合中添加或者更新元素
          map(elem) = map.getOrElse(elem,0)+1
      }
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
     //当前Excutor的map
     var map1 = map
     var map2 = other.value

     map = map1.foldLeft(map2) {
      (nm, kv) => {
        var k: String = kv._1
        var v: Int = kv._2
        nm(k) = nm.getOrElse(k, 0) + v
        nm
      }
    }
  }

  //获取累加器的值
  override def value: mutable.Map[String, Int] = map
}