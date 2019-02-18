/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.seanxia.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.SimpleConsumer;
import kafka.javaapi.consumer.ConsumerConnector;


public class MyConsumer extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;

	public MyConsumer(String topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
		props.put("group.id", "shsxt4");
		props.put("zookeeper.session.timeout.ms", "400");
//		props.put("auto.commit.interval.ms", "100");//
        props.put("auto.offset.reset","smallest");
        props.put("auto.commit.enable","false"); // 关闭自动提交，开启手动提交

		return new ConsumerConfig(props);

	}


// push消费方式，服务端推送过来。主动方式是pull
	public void run() {

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 描述读取哪个topic，需要几个线程读
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> list = consumerMap.get(topic);  // 每个线程对应于一个KafkaStream

        KafkaStream stream = list.get(0);


		ConsumerIterator<byte[], byte[]> it = stream.iterator();
        System.out.println("xixii................");
        while (it.hasNext()){
            String data = new String(it.next().message());
            System.out.println("开始处理数据 ...:"+ data);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("数据处理中..." + data);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("处理完数据..." + data);
            consumer.commitOffsets();
        }
			
	}


	public static void main(String[] args) {
		MyConsumer consumerThread = new MyConsumer("mytopic2");
		consumerThread.start();
	}
}
