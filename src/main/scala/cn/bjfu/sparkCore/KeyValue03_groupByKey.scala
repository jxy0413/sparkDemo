package cn.bjfu.sparkCore

object KeyValue03_groupByKey {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",2),("a",3),("b",2)))
    val group = rdd.groupByKey()
    group.collect().foreach(println)
    //(a,CompactBuffer(1, 3))
    //(b,CompactBuffer(2, 2))
    group.map(t=>(t._1,t._2.sum)).foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
