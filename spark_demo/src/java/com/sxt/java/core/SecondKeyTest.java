package com.sxt.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondKeyTest {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local")
                .setAppName("SecondarySortTest");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> secondRDD = sc.textFile("data/secondSort.txt");

        JavaPairRDD<SecondSortKey, String> secRDD = secondRDD.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String[] fields = line.split(" ");
                SecondSortKey secondSortKey = new SecondSortKey(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
                return new Tuple2<>(secondSortKey, line);
            }
        });

        secRDD.sortByKey().foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
            @Override
            public void call(Tuple2<SecondSortKey, String> tuple2) throws Exception {
                System.out.println(tuple2._2);
            }
        });

    }
}
