package com.sxt.Test;


import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.List;

public class SparkStreamingUpdateStateByKey {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf();

        conf.setMaster("local[2]").setAppName("stream");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));


        streamingContext.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("node01", 7777);

        JavaPairDStream<String, Integer> pairDStream = dStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {

                Integer value = 0;

                if(v2.isPresent()){

                    value = v2.get();

                    System.out.println( value  + "  --------------" );
                }else{
                    System.out.println( value + " --- -  false ------  ");
                }


                for(Integer i : v1){
                    System.out.println( i + " ---- --- -- - - -- - -- - -- - -- -");
                    value +=i;

                }
                return Optional.of(value);
            }
        }).print();

        streamingContext.start();

        streamingContext.awaitTermination();

        streamingContext.stop();


    }
}
