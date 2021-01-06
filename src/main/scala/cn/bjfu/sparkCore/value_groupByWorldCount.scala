package cn.bjfu.sparkCore

object value_groupByWorldCount {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val strRdd = sc.makeRDD(List("Hello","Scala","Scala","spark","hello","hbase"))

    val flatMapRdd = strRdd.flatMap(str=>{str.split(",")})

    //flatMapRdd.map(t=>{(t,1)}).reduceByKey(_+_).collect().foreach(println)
    val wordToSum = flatMapRdd.map(t => (t, 1)).groupBy(t => t._1).map {
      case (word, list) => {
        (word, list.size)
      }
    }
    wordToSum.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
