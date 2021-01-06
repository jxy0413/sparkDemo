package cn.bjfu.sparkCore

object DoubleValue04_zip {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(1,2,3),3)
    val rdd2 = sc.makeRDD(Array("a","b","c"),3)
    rdd1.zip(rdd2).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
