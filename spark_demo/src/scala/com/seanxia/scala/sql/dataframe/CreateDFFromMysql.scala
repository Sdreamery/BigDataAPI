package com.seanxia.spark.scala.sql.dataframe

import java.util.HashMap

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreateDFFromMysql {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("mysql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
      * 第一种方式读取Mysql数据库表创建DF
      */
    val options = new HashMap[String,String]();
    options.put("url", "jdbc:mysql://127.0.0.1:3306/spark")
    options.put("driver","com.mysql.jdbc.Driver")
    options.put("user","root")
    options.put("password", "123456")
    options.put("dbtable","person")

    val person = sqlContext.read.format("jdbc").options(options).load()
    person.show()

    // 创建临时表t1
    person.registerTempTable("t1")

    val reader = sqlContext.read.format("jdbc")
    reader.option("url", "jdbc:mysql://127.0.0.1:3306/spark")
    reader.option("driver","com.mysql.jdbc.Driver")
    reader.option("user","root")
    reader.option("password","123456")
    reader.option("dbtable", "score")

    val score = reader.load()
    score.show()

    // 创建临时表t2
    score.registerTempTable("t2")

    val result = sqlContext.sql("select t1.id, t1.name, t2.score from t1, t2 where t1.name = t2.name")
    result.show()

    sc.stop()
  }

}
