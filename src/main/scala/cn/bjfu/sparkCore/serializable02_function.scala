package cn.bjfu.sparkCore

import org.apache.spark.rdd.RDD

object serializable02_function {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //Dirver:算子以外的代码都是在Driver端进行的
    //Executor:算子里面的代码都是在Executor端执行

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    //创建一个Search对象
    val search = new Search("hello")
    //函数传递
    search.getMatch1(rdd).collect().foreach(println)
    //属性传递
    search.getMatch2(rdd).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
class Search(query:String) extends Serializable {
    def isMatch(s:String):Boolean = {
       s.contains(query)
    }

    //函数序列化过程
    def getMatch1(rdd:RDD[String]) : RDD[String] ={
       rdd.filter(isMatch)
    }

    //属性序列化过程
    def getMatch2(rdd:RDD[String]): RDD[String] = {
       rdd.filter(x => x.contains(this.query))
    }
}