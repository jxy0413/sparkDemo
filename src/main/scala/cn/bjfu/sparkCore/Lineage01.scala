package cn.bjfu.sparkCore

object Lineage01 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val fileRDD = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\sparkDemo\\input\\*.txt")
    println(fileRDD.toDebugString)
    println("-------------------------")

    val wordRDD = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("-------------------------")

    val mapRDD = wordRDD.map((_,1))
    println(mapRDD.toDebugString)
    println("-------------------------")

    val resultRDD = mapRDD.reduceByKey(_+_)
    println(resultRDD.toDebugString)
    println("-------------------------")

    resultRDD.collect()
    //4.关闭连接
    sc.stop()
  }
}
