package com.seanxia.spark.scala.sql.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreateDFFromJsonRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("jsonRDD")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val nameRDD = sc.makeRDD(Array(
      "{\"name\":\"zhangsan\",\"age\":18}",
      "{\"name\":\"lisi\",\"age\":19}",
      "{\"name\":\"wangwu\",\"age\":20}"
    ))

    val scoreRDD = sc.makeRDD(Array(
      "{\"name\":\"zhangsan\",\"score\":100}",
      "{\"name\":\"lisi\",\"score\":200}",
      "{\"name\":\"wangwu\",\"score\":300}"
    ))

    val nameDF = sqlContext.read.json(nameRDD)
    val scoreDF = sqlContext.read.json(scoreRDD)

    nameDF.registerTempTable("t1")
    scoreDF.registerTempTable("t2")

    val result = sqlContext.sql("select t1.name, t1.age, t2.score from t1, t2 where t1.name = t2.name")
    result.show()

    sc.stop()
  }
}
