package com.seanxia.spark.scala.sql.dataframe

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object CreateDFFromHiveCluster {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hive")
    val sc = new SparkContext(conf)

    // HiveContext是SQLContext的子类
    val hiveContext = new HiveContext(sc)

    // 使用spark实例库
    hiveContext.sql("use spark")
    // 表student_infos如果存在就删除
    hiveContext.sql("drop table if exists student_infos")

    // 在hive中创建student_infos表
    hiveContext.sql("create table if not exists student_infos (name string,age int) " +
      "row format delimited fields terminated by '\t'")
    // 加载数据
    hiveContext.sql("load data local inpath '/root/test/student_infos' into table student_infos")
    //第二种读取Hive表加载DF方式
//    hiveContext.table("student_infos")

    hiveContext.sql("drop table if exists student_scores")
    hiveContext.sql("create table if not exists student_scores (name string,score int) " +
      "row format delimited fields terminated by '\t'")
    hiveContext.sql("load data local inpath '/root/test/student_scores' into tablestudent_scores")

    /**
      * 查询表生成DataFrame
      */
    val df = hiveContext.sql("select si.name,si.age,ss.score from student_infos si," +
      "student_scores ss where si.name = ss.name")

    hiveContext.sql("drop table if exists good_student_infos")

    /**
      * 将结果写入到hive表中
      */
    df.write.mode(SaveMode.Overwrite).saveAsTable("good_student_infos")

    sc.stop()
  }

}
