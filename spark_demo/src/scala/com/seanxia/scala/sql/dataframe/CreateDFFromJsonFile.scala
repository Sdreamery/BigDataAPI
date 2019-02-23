package com.seanxia.spark.scala.sql.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreateDFFromJsonFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("jsonFile")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("./data/json")
//    val df = sqlContext.read.format("json").load("./data/json")

    df.show()
    df.printSchema()

    // select * from table
    df.select(df.col("name")).show()

    // select name from table where age>19
    df.select(df.col("name"), df.col("age")).where(df.col("age").gt(19)).show()

    // 注册临时表
    df.registerTempTable("jtable")

    val result = sqlContext.sql("select * from jtable")
    result.show()

    sc.stop()
  }
}
