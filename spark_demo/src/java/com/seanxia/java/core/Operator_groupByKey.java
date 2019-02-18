package com.seanxia.java.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Operator_groupByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("groupByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
				new Tuple2<>("a", 1),
				new Tuple2<>("a", 2),
				new Tuple2<>("b", 3),
				new Tuple2<>("c", 4),
				new Tuple2<>("d", 5),
				new Tuple2<>("d", 6)
			));
		
		JavaPairRDD<String, Iterable<Integer>> groupByKey = parallelizePairs.groupByKey();


		groupByKey.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {

                Iterable<Integer> integers = t._2;

                String key = t._1;

                Iterator<Integer> iterator = integers.iterator();

                while(iterator.hasNext()){
                    System.out.println("key " + key + "  :" + iterator.next());
                }

                System.out.println(t);
			}
		});

	}
}
