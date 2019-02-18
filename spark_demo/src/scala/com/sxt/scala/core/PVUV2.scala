package com.sxt.scala.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PVUV2 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setMaster("local").setAppName("pvuv")
        val context = new SparkContext(conf)
        val linesRDD = context.textFile("data/pvuvdata")
        //pv
        //(www.jd.com,1000)
        linesRDD.map(x=>{
            val fields: Array[String] = x.split("\t")
            (fields(5),1)
        }).reduceByKey((x,y)=>{x + y}).sortBy(_._2,false).foreach(println)

        //uv
        //(www.taobao.com,10.20.30.18)

        val groupRDD: RDD[(String, Iterable[String])] = linesRDD.map(x=>{
            val fields = x.split("\t")
            (fields(5),fields(0))
        }).groupByKey()

        groupRDD.map(x=>{
            val key  = x._1
            val size = x._2.toSet.size
            (key,size)
        }).sortBy(_._2,false)foreach(println)


    }

}
