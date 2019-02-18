package com.sxt.java.core;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class CarTracker {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("car tracker");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rowRDD = sc.textFile("data/monitor_flow_action");


        JavaRDD<String> filterRDD = rowRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.split("\t")[1].equals("0001");
            }
        });

        JavaRDD<String> mapRDD = filterRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1.split("\t")[3];
            }
        });

        List<String> cars = mapRDD.collect();

        final Broadcast<List<String>> broadcast = sc.broadcast(cars);

        JavaRDD<String> crackerRDD = rowRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return broadcast.value().contains(v1.split("\t")[3]);
            }
        });

        JavaPairRDD<String, String> rdd1 = crackerRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String row) throws Exception {
                return new Tuple2<>(row.split("\t")[3], row);
            }
        });

        rdd1.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                String car = tuple2._1;

                Iterator<String> iterator = tuple2._2.iterator();

                List<String> list = IteratorUtils.toList(iterator);

                Collections.sort(list, new Comparator<String>() {
                    @Override
                    public int compare(String row1, String row2) {
                        String time1 = row1.split("\t")[4];
                        String time2 = row2.split("\t")[4];
                        if(!time1.equals(time2)){

                            return time1.compareTo(time2);
                        }else {
                            return 0;
                        }

                    }
                });

                String tracker = "";

                for (int i = 0; i <list.size() ; i++) {
                    String row =  list.get(i);
                    //卡口ID
                    String monitorID =  row.split("\t")[1];

                    tracker += "->" + monitorID;
                }

                System.out.println(car + " = " + tracker.substring(2) );
            }
        });

    }
}
