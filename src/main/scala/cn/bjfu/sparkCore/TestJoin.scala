package cn.bjfu.sparkCore

object TestJoin {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List((1,"a"),(2,"b")))
    val rdd2 = sc.makeRDD(List((1,"1"),(2,"2")))
    val rdd3 = rdd1.join(rdd2)
    rdd3.filter{
      case(k,(v1,v2)) =>{
        k==1
      }
    }.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
