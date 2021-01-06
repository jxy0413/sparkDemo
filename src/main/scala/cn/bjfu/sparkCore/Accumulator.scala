package cn.bjfu.sparkCore

/**
  * 分布式只写变量
  */
object Accumulator {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD，单值RDD，不能在shuffle过程 并对其求和
//    val rdd = sc.makeRDD(List(1,2,3,4))
//    println(rdd.sum())
//    println(rdd.reduce(_+_))
    //存在shuffle,效率较低
    val rdd = sc.makeRDD(List(("a",1),("b",2),("a",3),("a",1),("b",2),("a",3)))
//    val resRdd = rdd.reduceByKey(_+_)
//    resRdd.map(_._2).collect().foreach(println)
    //如果定义的是一个普通变量，那么在Driver定义，Excutor会创建变量的副本，算子都是对副本进行操作，Driver端的变量不会更新
//    var sum:Int = 0
//    rdd.foreach{
//      case(word,count) => {
//           sum += count
//           println(sum)
//      }
//    }
    //累加器和普通的变量相比，会将Excutor端的结果，收集到Driver端进行汇总
//    println(sum)

    //创建累加器
    val sumAcc = sc.longAccumulator("myAcc")

    rdd.foreach{
      case(word,count) => {
         sumAcc.add(count)
         println("*****"+sumAcc.value)
      }
    }
    println(sumAcc.value)
    //4.关闭连接
    sc.stop()
  }
}
