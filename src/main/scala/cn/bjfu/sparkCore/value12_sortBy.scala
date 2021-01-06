package cn.bjfu.sparkCore

object value12_sortBy {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(2,1,4,5,6,3))
    //设置为正序
    val sortRdd = rdd.sortBy(num=>num)
    sortRdd.collect().foreach(println)
    //设置为逆序
    val sortRddDesc = rdd.sortBy(num=>num,false)
    sortRddDesc.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
