package com.seanxia.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.List;

public class BroadCast {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local").setAppName("broadcast");

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        //黑名单...
        List<String> list = new ArrayList<String>();

        list.add("shsxt");
        list.add("bjsxt");

        final Broadcast<List<String>> broadcast = context.broadcast(list);

        JavaRDD<String> linesRDD = context.textFile("data/word.txt");

        linesRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                List<String> list = broadcast.value();

                for(String word:list){
                   if(v1.contains(word)) {
                       return true;
                   }
                }

                return false;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
