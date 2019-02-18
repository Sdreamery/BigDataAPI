package com.seanxia.java.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
/**
 * reduce
 * 
 * 根据聚合逻辑聚合数据集中的每个元素。
 * @author root
 *
 */
public class Operator_reduce {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("reduce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1,2,3,4,5));

		Integer reduceResult = parallelize.reduce(
		        new Function2<Integer, Integer, Integer>() {

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println(" v1 : " + v1 + " v2 : " + v2);
                return v1+v2;
			}
		});

        System.out.println(reduceResult);
//
//        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("1","2","3","4"));
//
//        String reduceResult = parallelize.reduce(
//                new Function2<String, String, String>() {
//                    /**
//                     *
//                     */
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public String call(String v1, String v2) throws Exception {
//                        System.out.println("v1 : " + v1 + " v2: " + v2);
//                        return v1+ " ---" +v2;
//                    }
//                });
//		System.out.println(reduceResult);
		sc.stop();
	}
}
