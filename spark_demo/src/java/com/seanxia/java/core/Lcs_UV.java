package com.seanxia.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.master.Master;
import org.apache.spark.scheduler.DAGScheduler;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class Lcs_UV {
    private  int sum = 10;


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local").setAppName("pv");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<String> lineRDD = context.textFile("./data/pvuvdata");


        JavaPairRDD<String, Iterable<String>> rdd1 = lineRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {


                String url = s.split("\t")[5];
                String ip = s.split("\t")[0];
                return new Tuple2<>(url, ip);
            }
        }).groupByKey();

        rdd1.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple2) throws Exception {


                HashSet<Object> set = new HashSet<>();

                Iterator<String> iterator = tuple2._2.iterator();

                while(iterator.hasNext()){
                    set.add(iterator.next());
                }

                System.out.println(tuple2._1  + " : " + set.size());
            }
        });


        Map<String, Object> map = lineRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {

                String url = s.split("\t")[5];
                String ip = s.split("\t")[0];
                return new Tuple2<>(url, ip);
            }
        }).distinct().countByKey();

        for (String key : map.keySet()) {
            System.out.println("key : " + key + "   value :" + map.get(key));
        }


    }
}
