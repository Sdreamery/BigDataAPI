package com.seanxia.spark.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Properties;

public class CreateDFFromMysql {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mysql");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        /**
         * 第一种方式读取MySql数据库表，加载为DataFrame
         */

        HashMap<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://127.0.0.1:3306/spark");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "123456");
        options.put("dbtable", "person");

        DataFrame person = sqlContext.read().format("jdbc").options(options).load();
//        DataFrame person = sqlContext.read().jdbc();
        person.show();

        // 创建临时表
        person.registerTempTable("t1");

        /**
         * 第二种方式读取MySql数据表加载为DataFrame
         */
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url", "jdbc:mysql://127.0.0.1:3306/spark");
        reader.option("driver", "com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "123456");
        reader.option("dbtable", "score");

        DataFrame score = reader.load();
        score.show();

        // 创建临时表
        score.registerTempTable("t2");

        DataFrame result = sqlContext.sql("select t1.id, t1.name, t2.score from t1, t2 where person.name = score.name");

        result.show();

        /**
         * 将DataFrame结果保存到Mysql中
         */
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "123456");

        result.write().mode(SaveMode.Overwrite).
                jdbc("jdbc:mysql://127.0.0.1:3306/spark", "result", properties);

        sc.stop();
    }
}
