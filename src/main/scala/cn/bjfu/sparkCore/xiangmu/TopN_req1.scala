package cn.bjfu.sparkCore.xiangmu

/**
  * 需求1：统计热门品类TopN
  */
object TopN_req1 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //1、读取
    val dataRdd = sc.textFile("D:\\BaiduNetdiskDownload\\2.资料\\spark-core数据\\1.txt")
    //2、将读取的数据进行切分，将内容封装成UserVisitAction对象
    val actionRdd = dataRdd.map {
      line => {
        val fields = line.split("_")
        //封装对象
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    }
    //判断当前日志记录的是什么行为，并且封装为结果对象(品类,点击数,下单数，支付数)===》
    //列如：如果是鞋的点击行为 (鞋,1,0,0)
    actionRdd
    //4.关闭连接
    sc.stop()
  }
}

case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo(categoryId: String,//品类id
                             clickCount: Long,//点击次数
                             orderCount: Long,//订单次数
                             payCount: Long)//支付次数