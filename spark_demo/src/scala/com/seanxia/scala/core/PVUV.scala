package com.seanxia.scala.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Layne on 2018/5/4.
  */
object PVUV {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local").setAppName("pvuv")
        val sc = new SparkContext(conf)
        val records = sc.textFile("data/pvuvdata")

        //pv
        records.map(x => {
            val fields = x.split("\t")
            (fields(5), 1)
        }).reduceByKey(_ + _).sortBy(_._2).foreach(println)

        //uv
        val result: RDD[(String, Iterable[String])] = records.map(x => {
            val fields = x.split("\t")
            (fields(5), fields(0))
        }).groupByKey()

        result.foreach(x => {
            val key = x._1
            val iteratable = x._2

            println("key : " + key + " size : " + iteratable.toSet.size)
        })
    }
}
