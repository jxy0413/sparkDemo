package cn.bjfu.sparkCore
//广播变量 分布式只读变量
object Spark08_Broadcast {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Spark08_Broadcast").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //想实现类似json的效果 (a,(1,4)),(b,(2,5))
    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val list = List(("a",1),("b",2),("c",3))
    val broadcastList = sc.broadcast(list)
    rdd.map{
      case(k1,v1) => {
        for((k2,v2) <- broadcastList.value){
           if(k1==k2){

           }
        }
      }
    }

    //4.关闭连接
    sc.stop()
  }
}
