package com.sxt.java.sql.dataframe;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;


public class CreateDFFromMysql {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mysql");
        /**
         * 	配置join或者聚合操作shuffle数据时分区的数量
         */
        conf.set("spark.sql.shuffle.partitions", "1");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        /**
         * 第一种方式读取MySql数据库表，加载为DataFrame
         */
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://127.0.0.1:3306/spark");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "root");
        options.put("dbtable", "person");

        DataFrame person = sqlContext.read().format("jdbc").options(options).load();
        person.show();




        person.registerTempTable("person");
        /**
         * 第二种方式读取MySql数据表加载为DataFrame
         */

        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url", "jdbc:mysql://127.0.0.1:3306/spark");
        reader.option("driver", "com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "root");
        reader.option("dbtable", "score");
        DataFrame score = reader.load();
        score.show();
        score.registerTempTable("score");

        DataFrame result =
                sqlContext.sql("select person.id,person.name,person.age,score.score "
                        + "from person,score "
                        + "where person.name = score.name  and score.score> 90");
        result.show();

        result.registerTempTable("result");
//
        DataFrame df = sqlContext.sql("select id,name,age,score from result where age > 18");
        df.show();

        /**
         * 将DataFrame结果保存到Mysql中
         */




        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "root");
        /**
         * SaveMode:
         * Overwrite：覆盖
         * Append:追加
         * ErrorIfExists:如果存在就报错
         * Ignore:如果存在就忽略
         *
         */

        result.write().mode(SaveMode.Append).jdbc("jdbc:mysql://127.0.0.1:3306/spark", "result2", properties);
        System.out.println("----Finish----");
        sc.stop();


    }
}
