package cn.bjfu.sparkCore

object action06_takeOrdered {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,3,2,4))

    val result = rdd.takeOrdered(2)
    println(result.toList)
    //4.关闭连接
    sc.stop()
  }
}
