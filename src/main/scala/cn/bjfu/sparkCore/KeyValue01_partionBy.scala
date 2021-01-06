package cn.bjfu.sparkCore

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

object KeyValue01_partionBy {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc")),3)
    //自定义分区
    val rdd3 = rdd.partitionBy(new MyPartitioner(2))
    val indexRdd: RDD[(Int, String)] = rdd3.mapPartitionsWithIndex(
      (index, datas) => {
        // 打印每个分区数据，并带分区号
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        // 返回分区的数据
        datas
      }
    )
    indexRdd.collect()
    //4.关闭连接
    sc.stop()
  }
}
class MyPartitioner(num : Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
     if(key.isInstanceOf[Int]){
        val keyInt = key.asInstanceOf[Int]
        if(keyInt%2==0){
           0
        }else{
          1
        }
     }else{
       0
     }
  }
}