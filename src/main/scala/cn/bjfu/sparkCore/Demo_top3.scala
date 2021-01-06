package cn.bjfu.sparkCore
object Demo_top3 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRdd = sc.textFile("C:\\Users\\Administrator\\IdeaProjects\\sparkDemo\\input\\agent.txt")
    //3.将原始数据进行转换
    val preAndAdvToOneRdd = dataRdd.map {
      line => {
        val datas = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    }
    val prvAndAdvToSumRDD = preAndAdvToOneRdd.reduceByKey(_+_)

    val prvToAdvAndSumRdd = prvAndAdvToSumRDD.map {
      case (prvAndAdv, sum) => {
        val ks = prvAndAdv.split("-")
        (ks(0), (ks(1), sum))
      }
    }

    val groupRdd = prvToAdvAndSumRdd.groupByKey()

    val mapValueRdd = groupRdd.mapValues {
      data => {
        data.toList.sortWith(
          (l, r) => {
            l._2 > r._2
          }
        )
      }.take(3)
    }
    mapValueRdd.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
