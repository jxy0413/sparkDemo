package cn.bjfu.sparkCore

object value09_distinct {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val distnctRdd = sc.makeRDD(List(1,2,3,4,2,3,1))
    //对RDD去重后采用多个Task去重，提高并法度
    distnctRdd.distinct(2).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
