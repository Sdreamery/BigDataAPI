package com.sxt.java.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;

/**
 * 累加器在Driver端定义赋初始值和读取，在Executor端累加。
 * @author root
 *
 */
public class AccumulatorOperator {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("accumulator");

		JavaSparkContext sc = new JavaSparkContext(conf);
		final Accumulator<Integer> accumulator = sc.accumulator(0);

		sc.textFile("data/word.txt",2).foreach(new VoidFunction<String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				accumulator.add(1);
//				System.out.println(accumulator.value());
				System.out.println(accumulator);
			}
		});


        // accumulator.value 写法只能在driver端，excutor端的task只能用accumulator 写法来查看数据

		System.out.println(accumulator.value());
		sc.stop();
		
	}
}
