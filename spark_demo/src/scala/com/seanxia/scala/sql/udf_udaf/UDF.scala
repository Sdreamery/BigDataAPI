package com.seanxia.spark.scala.sql.udf_dfaf

import org.apache.spark.sql.types.{DataTypes, StringType, StructField}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("udf")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);

    val rdd = sc.makeRDD(Array("zhansan","lisi","wangwu"))
    val rowRDD = rdd.map { x => {
      RowFactory.create(x)
    } }

    val  schema  = DataTypes.createStructType(Array(StructField("name",StringType,true)))

    val df = sqlContext.createDataFrame(rowRDD, schema)

    df.registerTempTable("user")
    //sqlContext.udf.register("StrLen",(s : String)=>{s.length()})
    //sqlContext.sql("select name ,StrLen(name) as length from user").show

    sqlContext.udf.register("StrLen",(s : String,i:Int)=>{s.length()+i})
    sqlContext.sql("select name ,StrLen(name,10) as length from user").show
    sc.stop()
  }
}
