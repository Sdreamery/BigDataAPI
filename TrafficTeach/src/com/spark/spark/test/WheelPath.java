package com.spark.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 求经过某个卡口的车辆轨迹
 * 假如卡口为 0001
 */
public class WheelPath {
    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("wheelPath");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = sc.textFile("./monitor_flow_action");

        /**
         * 拿到卡口0001下所有车辆的车牌号
         */
        List<String> carList = lineRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                return new Tuple2<>(line.split("\t")[1], line);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {

                String monitorId = tuple2._1;
                return monitorId.equals("0001");
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2.split("\t")[3];
            }
        }).distinct().collect();

        // 将车牌号广播出去 car_broadcast
        final Broadcast<List<String>> car_broadcast = sc.broadcast(carList);

        /**
         * 拿到卡口号为0001下面所有车辆的信息
         * 通过 filter+广播变量
         */
        JavaPairRDD carInfoRDD = lineRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {

                return new Tuple2<>(line.split("\t")[3], line);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {

                String carId = tuple2._2.split("\t")[3];
                List<String> carList = car_broadcast.getValue();
                return carList.contains(carId);
            }
        }).groupByKey();

        /**
         * 取出每辆车经过的卡口号并按时间并排序
         * 这里优化，用TreeMap自动按照Key（时间）排序
         */
        carInfoRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                String carId = tuple2._1;
                String track = "";
                Iterator<String> rowIterator = tuple2._2.iterator();

                // 优化，直接放到TreeMap。自动按照 Key(时间) 排序
                TreeMap<String, String> map = new TreeMap<>();
                while (rowIterator.hasNext()){
                    String line = rowIterator.next();
                    String monitorId = line.split("\t")[1];
                    String time = line.split("\t")[4];
                    map.put(time, monitorId);
                }

                System.out.println("-----------------------------------------------------------------");
                System.out.println("carId:  " + carId);
                for (Map.Entry<String,String> entry : map.entrySet()) {
                    System.out.println("entry.getKey: " + entry.getKey() + "======>" + "entry.getValue: " + entry.getValue());
                    track = track + "->" + entry.getValue();
                }

//                List<Row> list = IteratorUtils.toList(rowIterator);
//                Collections.sort(list, new Comparator<Row>() {
//                    @Override
//                    public int compare(Row o1, Row o2) {
//                        String date1 = o1.getString(4);
//                        String date2 = o2.getString(4);
//
//                        boolean after = DateUtils.after(date1, date2);
//                        if (after){
//                            return 1;
//                        }
//                        return -1;
//                    }
//                });
//
//                for (Row row : list) {
//                    track = track + "->" + row.getString(1);
//                }
                System.out.println(carId + " : " + track.substring(2));
            }
        });
    }
}
