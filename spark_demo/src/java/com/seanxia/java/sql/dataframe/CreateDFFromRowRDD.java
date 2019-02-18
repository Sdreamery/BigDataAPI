package com.seanxia.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

public class CreateDFFromRowRDD {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("rddStruct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lineRDD = sc.textFile("data/person.txt");

        JavaRDD<Row> rowRDD = lineRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] values = v1.split(",");
                Row row = RowFactory.create(values[0], values[1], Integer.parseInt(values[2]));
                return row;
            }
        });


        List list = new ArrayList<>();
        //ctrl + shft + 上下键

        list.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        list.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        list.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
//        sqlContext.createDataFrame()

        StructType structType = DataTypes.createStructType(list);

        DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, structType);
        dataFrame.show();

    }
}
