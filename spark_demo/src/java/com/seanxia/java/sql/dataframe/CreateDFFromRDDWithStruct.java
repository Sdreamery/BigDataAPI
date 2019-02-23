package com.seanxia.spark.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bouncycastle.util.Integers;

import java.util.Arrays;
import java.util.List;

public class CreateDFFromRDDWithStruct {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("rddStruct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lineRDD = sc.textFile("./data/person.txt");

        /**
         * 转换成Row类型的RDD
         */
        JavaRDD<Row> rowRDD = lineRDD.map(new Function<String, Row>() {

            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(
                        s.split(",")[0],
                        s.split(",")[1],
                        Integer.valueOf(s.split(",")[0])
                );
            }
        });

        /**
         * 动态构建DataFrame中的元数据，
         * 一般来说这里的字段可以来源自字符串，也可以来源于外部数据库
         */
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("aname", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType schema = DataTypes.createStructType(structFields);

        DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
        df.printSchema();
        df.show();

        sc.stop();
    }
}
