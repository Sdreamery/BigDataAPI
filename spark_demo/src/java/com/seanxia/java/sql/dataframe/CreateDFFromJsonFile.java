package com.seanxia.spark.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDFFromJsonFile {
    public static void main(String[] args){

        // 配置上下文环境
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("jsonFile");
        SparkContext sc = new SparkContext(conf);

        // 创建sqlContext
        SQLContext sqlContext = new SQLContext(sc);

        /**
         * DataFrame的底层是一个一个的RDD RDD的泛型是Row类型。
         * 以下两种方式都可以读取json格式的文件
         */
        DataFrame df = sqlContext.read().format("json").load("./data/json");
//        DataFrame df = sqlContext.read().json("./data/json");

        /**
         * 显示 DataFrame中的内容，默认显示前20行。如果现实多行要指定多少行show(行数)
         * 注意：当有多个列时，显示的列先后顺序是按列的ascii码先后显示。
         */
//        df.show();

        // dataFrame转换成RDD，两种方式
        RDD<Row> rdd = df.rdd();
//        JavaRDD<Row> rowJavaRDD = df.javaRDD();

        // 树形形式显示scahema信息
//        df.printSchema();

        /**
         * dataFram自带的API 操作DataFrame
         */
        // select name ,age from table where age>18
        df.select(df.col("name"),df.col("age")).where(df.col("age").gt(18)).show();

        //select count(*) from table group by age
        df.groupBy(df.col("age")).count().show();

        /**
         * 将DataFrame注册成临时的一张表，
         * 这张表临时注册到内存中，是逻辑上的表，不会雾化到磁盘
         */
        df.registerTempTable("jtable");
        DataFrame result = sqlContext.sql("select age, count(1) from jtable group by age");
        result.show();

        sc.stop();
    }

}
