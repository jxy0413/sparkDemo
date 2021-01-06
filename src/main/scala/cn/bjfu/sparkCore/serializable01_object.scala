package cn.bjfu.sparkCore

object serializable01_object {
  def main(args: Array[String]): Unit = {
        import org.apache.spark.{SparkConf, SparkContext}
        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)
        val user = new User
        user.name ="jia"
        val user1 = new User()
        user1.name = "shiqi"
        val userRdd = sc.makeRDD(List(user,user1))
        userRdd.foreach(user => println(user.name))
        //4.关闭连接
        sc.stop()
  }
}
class User extends Serializable {
    var name : String = _
}