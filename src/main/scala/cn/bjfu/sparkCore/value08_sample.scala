package cn.bjfu.sparkCore

object value08_sample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 10,1)

    rdd.sample(true,0.4,2).collect().foreach(println)
    println("**********************")
    rdd.sample(false,0.4,2).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
