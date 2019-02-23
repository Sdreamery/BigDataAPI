package com.seanxia.spark.java.sparkstreaming;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;


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
        conf.put("metadata.broker.list", "sean01:9092,sean02:9092,sean03:9092");
        conf.put("serializer.class", StringEncoder.class.getName());
        conf.put("acks",1);

        producerForKafka = new Producer<Integer, String>(new ProducerConfig(conf));
    }

    @Override
    public void run() {
        int counter = 0;
        while (true) {
            counter++;
            String value = "seanxia";
            KeyedMessage<Integer, String> message = new KeyedMessage<>(topic, value);
            producerForKafka.send(message);
            System.out.println(value + " : " +counter + " ---------------------------");

            //hash partitioner 当有key时，则默认通过key 取hash后 ，对partition_number 取余数
//			producerForKafka.send(new KeyedMessage<Integer, String>(topic,22,userLog));
//            每2条数据暂停2秒
            if (0 == counter % 2) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new MyProducer("sk1").start();
        new MyProducer("sk2").start();
    }

}
