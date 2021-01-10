package cn.bjfu.sparkCore.xiangmu

import scala.collection.mutable.ListBuffer

/**
  * 需求2：统计热门品类TopN中，活跃用户的topN
  */
object TopN_req2 {
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
    //(鞋,1,0,0)
    //(保健品,1,0,0)
    //(鞋,0,1,0)
    //(鞋,0,0,1)
    val cationInfoRdd = actionRdd.flatMap {
      userAction => {
        //判断是否为点击行为
        if (userAction.click_category_id != -1) {
          //封装输出结果对象
          List(CategoryCountInfo(userAction.click_category_id.toString, 1, 0, 0))
        } else if (userAction.order_category_ids != "null") { //坑：读取的文件应该是null字符串 而不是null对象
          //判断是否为下单行为
          //如果是下单行为，需要对当前订单中设计的所有品类id进行切分
          val ids = userAction.order_category_ids.split(",")
          //对所有的品类的id 进行遍历
          //定义一个集合，用于存放多个品类id封装输出的结果对象
          val countInfoList = ListBuffer[CategoryCountInfo]()
          for (id <- ids) {
            countInfoList.append(CategoryCountInfo(id, 0, 1, 0))
          }
          countInfoList
        } else if (userAction.pay_category_ids != "null") {
          //支付行为
          val ids = userAction.pay_category_ids.split(",")
          //对所有的品类的id 进行遍历
          //定义一个集合，用于存放多个品类id封装输出的结果对象
          val countInfoList = ListBuffer[CategoryCountInfo]()
          for (id <- ids) {
            countInfoList.append(CategoryCountInfo(id, 0, 0, 1))
          }
          countInfoList
        } else {
          Nil
        }
      }
    }
    val groupRdd = cationInfoRdd.groupBy(_.categoryId)
    //将分组之后的数据进行聚合处理
    val reduceRdd = groupRdd.mapValues {
      datas => {
        datas.reduce {
          (info1, info2) => {
            info1.clickCount = info1.clickCount + info2.clickCount
            info1.orderCount = info1.orderCount + info2.orderCount
            info1.payCount = info1.payCount + info2.payCount
            info1
          }
        }
      }
    }
    //对上述的Rdd的结构进行转换，只保留value部分,得到聚合之后的Rdd
    val mapRDD = reduceRdd.map(_._2)
    //reduceRdd.collect().foreach(println)
    //对Rdd中的数据进行排序
    val res = mapRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false).take(10)

    //=====================需求二=============================
    //1、获取热门品类的top10的品类id
    val categoryIdRdd = res.map(_.categoryId)
    //println(categoryIdRdd.mkString(","))
    //对原始数据进行过滤
    //ids可以进行优化
    val broadcastIds = sc.broadcast(categoryIdRdd)
    val filterRdd = actionRdd.filter {
      action => {
        //只保留点击的行为
        if (action.click_category_id != -1) {
          //同时确定是热门平类的点击
          //集合数据为字符串类型，id是Long类型，需要进行转换
          broadcastIds.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    }
    //对session的点击数量进行转换
    val mapRdd1 = filterRdd.map {
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    }
    //mapRdd1.collect().foreach(println)
    //(11_de45a822-fd78-42df-a80c-ef367c66b64f,1)
    //(15_d79508b4-66bf-4410-a5bb-f67a8831610f,1)
    //(19_d79508b4-66bf-4410-a5bb-f67a8831610f,1)
    //(9_d79508b4-66bf-4410-a5bb-f67a8831610f,1)
    val reduceRdd1 = mapRdd1.reduceByKey(_+_)
    val mapRdd2 = reduceRdd1.map {
      case (categroryAndsession, sum) => {
        (categroryAndsession.split("_")(0), (categroryAndsession.split("_")(1), sum))
      }
    }
    val groupRdd2 = mapRdd2.groupByKey()

    val resRDD = groupRdd2.mapValues {
      datas => {
        datas.toList.sortWith {
          case (l, r) => {
            l._2 > r._2
          }
        }.take(10)
      }
    }
    resRDD.foreach(println)
    //4.关闭连接
    sc.stop()
  }
}