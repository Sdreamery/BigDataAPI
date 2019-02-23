package com.seanxia.spark.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

public class CreateDFFromJsonRDD {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("jsonRDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"age\":\"18\"}",
                "{\"name\":\"lisi\",\"age\":\"19\"}",
                "{\"name\":\"wangwu\",\"age\":\"20\"}"
        ));

        JavaRDD<String> scoreRDD = sc.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"score\":\"100\"}",
                "{\"name\":\"lisi\",\"score\":\"200\"}",
                "{\"name\":\"wangwu\",\"score\":\"300\"}"
        ));

        DataFrame namedf = sqlContext.read().json(nameRDD);
        DataFrame scoredf = sqlContext.read().json(scoreRDD);

        namedf.registerTempTable("t1");
        scoredf.registerTempTable("t2");

        DataFrame result = sqlContext.sql("select t1.name, t1.age, t2.score from t1, t2 where t1.name=t2.name ");
        result.show();

        sc.stop();
    }

}
