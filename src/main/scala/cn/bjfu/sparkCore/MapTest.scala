package cn.bjfu.sparkCore

import scala.collection.mutable

object MapTest {
  def main(args: Array[String]): Unit = {
     val map1 = mutable.Map[String,Int]("key1"->1,"key2"->2)
     val map2 = mutable.Map[String,Int]("key1"->4,"key4"->3)
     //List(1, 2, 3, 4).foldLeft(0)((sum, i) => sum + i)
     val mapAdd = map1.foldLeft(map2){
       (nm,kv) =>{
           var k:String = kv._1
           var v:Int = kv._2
           nm(k) = nm.getOrElse(k,0)+v
           nm
       }
     }
     println(mapAdd.toString())
     println(mapAdd("key1"))
  }
}
