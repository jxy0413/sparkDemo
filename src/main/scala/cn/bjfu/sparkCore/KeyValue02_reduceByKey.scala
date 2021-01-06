package cn.bjfu.sparkCore

object KeyValue02_reduceByKey {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",2),("b",3),("a",3),("b",4)))
    val reduceRdd = rdd.reduceByKey((x,y)=> x+y)
    reduceRdd.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
