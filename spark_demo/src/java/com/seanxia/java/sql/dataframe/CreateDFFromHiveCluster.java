package com.seanxia.spark.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class CreateDFFromHiveCluster {
    public static void main(String[] args){

        SparkConf conf = new SparkConf().setAppName("hive");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // HiveContext是SQLContext的子类
        HiveContext hiveContext = new HiveContext(sc);

        // 使用spark实例库
        hiveContext.sql("USE spark");
        // 表student_infos如果存在就删除
        hiveContext.sql("DROP TABLE IF EXISTS student_infos");

        // 在hive中创建student_infos表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos " +
                "(name STRING,age INT) row format delimited fields terminated by '\t' ");
        // 加载数据
        hiveContext.sql("load data local inpath '/root/test/student_infos' into table student_infos");
        //第二种读取Hive表加载DF方式
//        hiveContext.table("student_infos");

        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        // 在hive中创建student_scores表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores " +
                "(name STRING, score INT) row format delimited fields terminated by '\t'");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/root/test/student_scores' INTO TABLE student_scores");

        /**
         * 查询表生成DataFrame
         */
        DataFrame goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss "
                + "ON si.name=ss.name "
                + "WHERE ss.score>=80");

        // 注册临时表
        goodStudentsDF.registerTempTable("goodStudent");
        DataFrame result = hiveContext.sql("select * from goodStudent");
        result.show();

        /**
         * 将结果保存到hive表，good_student_infos
         */
        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
        goodStudentsDF.write().mode(SaveMode.Overwrite).saveAsTable("good_student_infos");

        DataFrame table = hiveContext.table("good_student_infos");
        Row[] goodStudentRows = table.collect();
        for (Row goodStudentRow : goodStudentRows) {
            System.out.println(goodStudentRow);
        }

        sc.stop();
    }

}
