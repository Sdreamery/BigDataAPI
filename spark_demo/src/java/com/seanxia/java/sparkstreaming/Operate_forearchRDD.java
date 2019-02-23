package com.seanxia.spark.java.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * foreachRDD  算子注意：
 *  1.foreachRDD是DStream中output operator类算子
 *  2.foreachRDD可以遍历得到DStream中的RDD，可以在这个算子内对RDD使用RDD的Transformation类算子进行转化，但是一定要使用rdd的Action类算子触发执行。
 *  3.foreachRDD可以得到DStream中的RDD，在这个算子内，RDD算子外执行的代码是在Driver端执行的，RDD算子内的代码是在Executor中执行。
 *
 */

public class Operate_forearchRDD {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Operate_forearchRDD");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		JavaDStream<String> dStream = jsc.socketTextStream("sean01", 8888);


		dStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<String> rdd) throws Exception {

                // 这里是driver端运行  可以写一些广播变量....
                System.out.println("driver.............");
                SparkContext context = rdd.context();

                //方法：实时去读黑名单...
//                context.broadcast()
                long countRDD = rdd.filter(new Function<String, Boolean>() {
                    private static final long serialVersionUID = -3517418467535661859L;

                    @Override
                    public Boolean call(String v1) throws Exception {
                        System.out.println("==========================");
                        return v1.contains("seanxia");
                    }
                }).count();

            }
		});

		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}
