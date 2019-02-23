package com.seanxia.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSortApp {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("SecondSortTest");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = context.textFile("./data/secondSort.txt");

        JavaPairRDD<SecondSortKey, String> pairSecondRDD = lineRDD.mapToPair(
                new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String[] split = line.split(" ");
                Integer first = Integer.valueOf(split[0]);
                Integer second = Integer.valueOf(split[1]);
                SecondSortKey secondSortKey = new SecondSortKey(first, second);
                return new Tuple2<SecondSortKey, String>(secondSortKey, line);
            }
        });

        pairSecondRDD.sortByKey(false).foreach(
                new VoidFunction<Tuple2<SecondSortKey, String>>() {
            @Override
            public void call(Tuple2<SecondSortKey, String> tuple2) throws Exception {
                System.out.println(tuple2 + "------" + tuple2._2);
            }
        });

    }


}
