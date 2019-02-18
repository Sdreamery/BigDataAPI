package com.sxt.scala

import org.apache.spark.{SparkConf, SparkContext}

object CarInfor {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local").setAppName("car info")

        val context = new SparkContext(conf)


        var rowRDD = context.textFile("data/monitor_flow_action")

        rowRDD = rowRDD.cache()

        val monitorIDRDD = rowRDD.map(x=>{
            //(0001,1)
            //(0001,1)
            //(0002,1)
            (x.split("\t")(1),1)
        })
//        val map: collection.Map[String, Long] = monitorIDRDD.countByKey()
//        val map: collection.Map[(String, Int), Long] = monitorIDRDD.countByValue()
        //(0001,540) (0002,200)
        val monitorTuple: Array[(String, Int)] = monitorIDRDD.reduceByKey(_+_).sortBy(_._2,false).take(3)

        val monitorIds: Array[String] = monitorTuple.map(x=>x._1)

        val broadcast = context.broadcast(monitorIds)

        rowRDD.filter(x=>{

            val monitorId = x.split("\t")(1)
            val ids = broadcast.value
            ids.contains(monitorId)

        }).map(x=>{

            (x.split("\t")(1),x.split("\t")(3))

        }).distinct().groupByKey().foreach(x=>{
            val monitorID = x._1
            print(monitorID + " = ")
            val iterator = x._2.iterator
            //车牌号
            iterator.foreach(x=> {
                print(x + " ")
            } )
            println()
        })

    }

}
