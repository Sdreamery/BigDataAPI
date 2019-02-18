package com.sxt.Test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * 向kafka中生产数据
 *
 * @author root
 */
public class MyProducer extends Thread {
    // sparkstreaming  storm     flink 两三年后变成主流  流式处理，可能更复杂，数据处理性能要非常好


    private String topic; //发送给Kafka的数据,topic
    private Producer<Integer, String> producerForKafka;


    public MyProducer(String topic) {

        this.topic = topic;

        Properties conf = new Properties();
        conf.put("metadata.broker.list", "node01:9092,node02:9092,node03:9092");
        conf.put("serializer.class", StringEncoder.class.getName());
        conf.put("acks",1);

        producerForKafka = new Producer<Integer, String>(new ProducerConfig(conf));
    }


    @Override
    public void run() {
        int counter = 0;
        while (true) {
            counter++;
            String value = "shsxt" + counter;

            KeyedMessage<Integer, String> message = new KeyedMessage<>(topic, value);

            producerForKafka.send(message);
            System.out.println(value + " - -- -- --- -- - -- - -");

            //hash partitioner 当有key时，则默认通过key 取hash后 ，对partition_number 取余数
//			producerForKafka.send(new KeyedMessage<Integer, String>(topic,22,userLog));
//            每2条数据暂停2秒
            if (0 == counter % 2) {

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {

        new MyProducer("mytopic2").start();

    }




}
