package com.zhileiedu.kafaka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author: wzl
 * @Date: 2020/3/4 16:40
 */
public class MyConsumer {
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "hadoop01:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true"); // 开启自动提交offset
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);


		// 订阅一个topic
		consumer.subscribe(Collections.singleton("second"));

		// 接取消息
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				System.out.println(consumerRecord.topic() + "话题在第"
						+ consumerRecord.partition() +
						"分区第" + consumerRecord.offset() +
						"条数据,内容是=" + consumerRecord.value());
			}
		}


		//consumer.close();


	}
}
