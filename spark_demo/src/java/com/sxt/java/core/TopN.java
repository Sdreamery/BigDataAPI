package com.sxt.java.core;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;



import java.util.*;


public class TopN {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();

        conf.setMaster("local[5]").setAppName("TopOps");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //hdfs://shsxt/wc.txt
        JavaRDD<String> linesRDD = sc.textFile("data/scores.txt",5);

        final List n = new ArrayList();
//      linesRDD.count();

        JavaPairRDD<String, Integer> pairRDD = linesRDD.mapToPair(
                new PairFunction<String, String, Integer>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String str) throws Exception {

                        String[] splited = str.split(" ");
                        String clazzName = splited[0];
                        Integer score = Integer.valueOf(splited[1]);
                        return new Tuple2<String, Integer>(clazzName, score);
                    }
                });

        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = pairRDD.groupByKey();

        groupByKeyRDD.foreach(
                new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {

                        String clazzName = tuple._1;
                        Iterator<Integer> iterator = tuple._2.iterator();
                        System.out.println(tuple);

                        Integer[] top3 = new Integer[3];

                        while (iterator.hasNext()) {

                            Integer score = iterator.next();

                            for (int i = 0; i < top3.length; i++) {
                                if (top3[i] == null) {
                                    top3[i] = score;
                                    break;
                                } else if (score > top3[i]) {
                                    for (int j = 2; j > i; j--) {
                                        top3[j] = top3[j - 1];
                                    }
                                    top3[i] = score;
                                    break;
                                }
                            }
                        }
                        System.out.println("class Name:" + clazzName);
                        for (Integer sscore : top3) {
                            System.out.println(sscore);
                        }

                    }
                });

//        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
//                String key  = tuple2._1;
//                Iterator<Integer> iterator = tuple2._2.iterator();
//                List list = IteratorUtils.toList(iterator);
//                Collections.sort(list);
//                for(int i=0;i<Math.min(3,list.size());i++){
//                    // list.size = 3  list.get(2)
//                    System.out.println(key + " " +  list.get(list.size()-i-1));
//                }
//            }
//        });

    }
}



