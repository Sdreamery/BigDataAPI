package com.seanxia.spark.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class CreateDFFromParquet {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("parquet");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> jsonRDD = sc.textFile("./data/json");
        DataFrame df = sqlContext.read().json(jsonRDD);

        /**
         * 将DataFrame保存成parquet文件，SaveMode指定存储文件时的保存模式
         * 保存成parquet文件有以下两种方式：
         */
        df.write().mode(SaveMode.Overwrite).format("parquet").save("./data/parquet");
        df.write().mode(SaveMode.Overwrite).parquet("./data/parquet");

        df.show();

        /**
         * 加载parquet文件成DataFrame
         * 加载parquet文件有以下两种方式：
         */
        DataFrame parquet = sqlContext.read().format("parquet").load("./data/parquet");
//        DataFrame parquet = sqlContext.read().parquet("./data/parquet");

        parquet.show();

        sc.stop();
    }
}
