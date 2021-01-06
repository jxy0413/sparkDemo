package cn.bjfu.sparkCore

object DoubleValue03_intersection {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.具体业务逻辑
    val rdd1 = sc.makeRDD(1 to 4)
    val rdd2 = sc.makeRDD(4 to 19)
    rdd1.intersection(rdd2).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
