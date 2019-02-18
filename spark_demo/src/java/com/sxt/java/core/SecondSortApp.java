package com.sxt.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Comparator;

public class SecondSortApp {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local").setAppName("SecondarySortTest");

        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> secondRDD = sc.textFile("data/secondSort.txt");

        JavaPairRDD<SecondSortKey, String> pairSecondRDD = secondRDD.mapToPair(
                new PairFunction<String, SecondSortKey, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<SecondSortKey, String> call(String line) throws Exception {
                String[] splited = line.split(" ");
                int first = Integer.valueOf(splited[0]);
                int second = Integer.valueOf(splited[1]);
                SecondSortKey secondSortKey = new SecondSortKey(first, second);
                return new Tuple2<SecondSortKey, String>(secondSortKey, line);
            }
        });

        pairSecondRDD.sortByKey(false).foreach(
                new VoidFunction<Tuple2<SecondSortKey, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Tuple2<SecondSortKey, String> tuple) throws Exception {
                        System.out.println(tuple + "------" + tuple._2);
                    }
                });

    }

}
