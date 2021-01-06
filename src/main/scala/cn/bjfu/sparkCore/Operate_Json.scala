package cn.bjfu.sparkCore

object Operate_Json {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val jsonRdd = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\sparkDemo\\input\\3.txt")
    import scala.util.parsing.json.JSON
    val resultRdd = jsonRdd.map(JSON.parseFull)
    resultRdd.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
