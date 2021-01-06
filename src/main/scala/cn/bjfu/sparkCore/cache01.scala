package cn.bjfu.sparkCore

import org.apache.spark.storage.StorageLevel

object cache01 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineWordRdd = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\sparkDemo\\input\\1.txt")

    val wordRdd = lineWordRdd.flatMap(_.split(" "))

    val wordToRdd = wordRdd.map {
      word => {
        println("****************************")
        (word, 1)
      }
    }

    println(wordToRdd.toDebugString)
    wordToRdd.cache()
    wordToRdd.persist(StorageLevel.MEMORY_AND_DISK_2)

    wordToRdd.collect()
    println("--------------------")
    println(wordToRdd.toDebugString)

    wordToRdd.collect()
    //4.关闭连接
    sc.stop()
  }
}
