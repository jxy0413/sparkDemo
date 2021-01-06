package cn.bjfu.sparkCore

object KeyValue01_partitionBy {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)

    val rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))

    println(rdd2.partitions.size)
    //4.关闭连接
    sc.stop()
  }
}
