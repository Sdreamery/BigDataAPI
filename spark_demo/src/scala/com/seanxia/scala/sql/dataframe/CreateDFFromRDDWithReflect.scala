package com.seanxia.spark.scala.sql.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

case class Person(id: String, name: String, age: Integer)

object CreateDFFromRDDWithReflect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("reflectRDD")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val lineRDD = sc.textFile("./data/person.txt")

    // 将RDD隐式转换成DataFrame
    import sqlContext.implicits._
    val personRDD = lineRDD.map(x => {
      val person = Person(
        x.split(",")(0),
        x.split(",")(1),
        Integer.valueOf(x.split(",")(2)
        ))
      person
    })

    val df = personRDD.toDF()

    df.show()

    /**
      * 将DataFrame转换成PersonRDD
      */
    val rdd = df.rdd
    val result = rdd.map(x => {
      Person(x.getAs("id"), x.getAs("name"), x.getAs("age"))
    })
    result.foreach(println)

    sc.stop()
  }

}
