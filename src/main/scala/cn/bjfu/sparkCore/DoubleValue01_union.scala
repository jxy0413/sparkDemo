package cn.bjfu.sparkCore

object DoubleValue01_union {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体逻辑代码
    val rdd1 = sc.makeRDD(List(1 to 4))
    val rdd2 = sc.makeRDD(List(5 to 10))

    val rdd3 = rdd1.union(rdd2)

    rdd3.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
