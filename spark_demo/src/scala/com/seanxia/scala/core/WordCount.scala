package com.seanxia.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    /**
      * 几种运行方式：
      *   1.本地运行
      *   2.yarn
      *   3.standalone
      *   4.mesos
      */
    conf.setMaster("local").setAppName("wc")

    val context = new SparkContext(conf)

    val lineRDD = context.textFile("./wc.txt")

    val wordRDD = lineRDD.flatMap(x => {x.split(" ")})

    val KVRDD = wordRDD.map(x => {
      println("=================")
      (x,1)
    })

    val resultRDD = KVRDD.reduceByKey((x,y) => {x+y})

    val sortRDD = resultRDD.sortBy(_._2,false)

    sortRDD.foreach(println)

  }

}
