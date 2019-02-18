package com.sxt.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class CreateDFFromJsonFile {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("jsonfile");

        JavaSparkContext context = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(context);

        DataFrame dataFrame = sqlContext.read().json("data/json");

        dataFrame.show();

        dataFrame.printSchema();

//        dataFrame.select("name").show();
//        //select name,age from table where age > 18;
//        dataFrame.select(dataFrame.col("name"),dataFrame.col("age").gt(18)).show();
//        //select name,age from table where age > 18;
//        dataFrame.select("name","age").where(dataFrame.col("age").gt(18)).show();
        //注册成内存中的临时表
        dataFrame.registerTempTable("person");
        //结果集
        DataFrame sql = sqlContext.sql("select name,age from person where age > 18");


        sql.show();


    }
}
