package cn.bjfu.sparkCore

object value04_flatMap {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val listRdd = sc.makeRDD(List(List(1,2),List(2,3),List(4,5)))

    listRdd.flatMap(list=>list).collect.foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
