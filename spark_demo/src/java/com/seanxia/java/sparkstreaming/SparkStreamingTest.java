package com.seanxia.spark.java.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingTest {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName("sparkStreaming");
//        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("sean01", 8888);

        JavaDStream<String> wordDStream = dStream.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 5302655187358849615L;

            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }
        });

        JavaPairDStream<String, Integer> pairDStream = wordDStream.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1285374334768880064L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> resultDStream = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 5889114600370649292L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        resultDStream.print();

        streamingContext.start();

        streamingContext.awaitTermination();

        streamingContext.stop();

    }

}
