package com.seanxia.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class UV {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();

        conf.setMaster("local").setAppName("pv");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = context.textFile("./data/pvuvdata");

        JavaPairRDD<String, String> urlipRDD = lineRDD.mapToPair(
                new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split("\t")[5], s.split("\t")[0]);

            }
        });

        JavaPairRDD<String, Iterable<String>> groupByKeyRDD = urlipRDD.groupByKey();


        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                HashSet<Object> set = new HashSet<>();

                Iterator<String> iterator = tuple2._2.iterator();

                while(iterator.hasNext()){
                    set.add(iterator.next());
                }

                System.out.println("url : " + tuple2._1  + " value: " + set.size());

            }
        });

        context.stop();
    }
}
