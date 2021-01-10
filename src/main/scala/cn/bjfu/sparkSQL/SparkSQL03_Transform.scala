package cn.bjfu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[1]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",40),(3,"wangwu",20)))
    //转为DF
    import spark.implicits._
    //进行转换之前，需要引入隐式转换规则
    //这里的spark不是包名的含义，是SparkSessiond对象的名字
    val df = rdd.toDF("id","name","age")
    //转为DS
    val ds = df.as[User]
    //转为DF
    val df1 = ds.toDF()
    //转为RDD
    val rdd1 = df1.rdd
    rdd1.foreach(row=>{
      //获取数据时，可以通过索引访问数据
      print(row.getInt(0)+"  ")
      print(row.getString(1)+"  ")
      print(row.getInt(2))
      println()
      println("*******************************")
    })
  }
}
case class User(id:Int,name:String,age:Int)