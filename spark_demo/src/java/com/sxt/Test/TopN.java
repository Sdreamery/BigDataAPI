package com.sxt.Test;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setMaster("local").setAppName("pv");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = context.textFile("./data/scores.txt");

        JavaPairRDD<String, Integer> pairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {

                Integer value = Integer.valueOf(line.split(" ")[1]);
                return new Tuple2<>(line.split(" ")[0], value);
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupByKey = pairRDD.groupByKey();

        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                String key = tuple2._1;

                Iterable<Integer> iterable = tuple2._2;

                ArrayList<Integer> l2 = new ArrayList<>();

                while(iterable.iterator().hasNext()){
                    l2.add(iterable.iterator().next());
                }

                List<Integer> list = IteratorUtils.toList(iterable.iterator());

                Collections.sort(list);

                for(int i =list.size()-1;i>=0;i--){
                    if(i>=(list.size()-3)){
                        System.out.println(key + " : " + list.get(i) );
                    }

                }

//                Collections.sort(list, new Comparator<Integer>() {
//                    @Override
//                    public int compare(Integer o1, Integer o2) {
//                        return 0;
//                    }
//                });
            }
        });


    }
}
