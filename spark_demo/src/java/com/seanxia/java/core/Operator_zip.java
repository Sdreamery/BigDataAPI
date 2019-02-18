package com.seanxia.java.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Operator_zip {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("zip");
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList("zhangsan","lisi","wangwu"));
//		JavaRDD<Integer> scoreRDD = sc.parallelize(Arrays.asList(100,200,300));
////		JavaRDD<Integer> scoreRDD = sc.parallelize(Arrays.asList(100,200,300,400));
//		JavaPairRDD<String, Integer> zip = nameRDD.zip(scoreRDD);
//		zip.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//
//			/**
//			 *
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<String, Integer> tuple) throws Exception {
//				System.out.println("tuple --- " + tuple);
//			}
//		});
		
		JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
				new Tuple2<String, String >("a","aaa"),
				new Tuple2<String, String >("b","bbb"),
				new Tuple2<String, String >("c","ccc")
				));
		JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
				new Tuple2<String, String >("1","111"),
				new Tuple2<String, String >("2","222"),
				new Tuple2<String, String >("3","333")
				));
		JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> result = rdd1.zip(rdd2);
        result.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>() {
            @Override
            public void call(Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple2Tuple2Tuple2) throws Exception {
                System.out.println(tuple2Tuple2Tuple2);
            }
        });
		sc.stop();
	}
}
