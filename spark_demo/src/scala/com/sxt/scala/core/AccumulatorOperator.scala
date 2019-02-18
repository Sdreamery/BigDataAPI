package com.sxt.scala.core

import org.apache.spark.Accumulable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker

object AccumulatorOperator {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local").setAppName("accumulator")

        val sc = new SparkContext(conf)
        val accumulator = sc.accumulator(0)
        //java创建对象的几种方式： 1.new  2.clone 3.反射  4.反序列化
        var count = 0
        sc.textFile("data/word.txt",2).foreach { x => {
            count += 1
            println( " count : " + count)
        }}


        sc.textFile("data/word.txt",2).foreach { x => {
            accumulator.add(1)

        }}

        println("accumulator.value : " + accumulator.value)
        println("自定义 : " + count)
        sc.stop()
    }
}