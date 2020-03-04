package com.zhileiedu.kafaka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @Author: wzl
 * @Date: 2020/3/4 16:40
 */
public class MyConsumerOffsetSave {

	// offset内存中的缓存
	private static Map<TopicPartition, Long> offset = new HashMap<TopicPartition, Long>();

	public static void main(String[] args) {

		final Properties props = new Properties();
		props.put("bootstrap.servers", "hadoop01:9092");
		props.put("group.id", "test");
		//props.put("enable.auto.commit", "true"); // 开启自动提交offset
		//props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// 订阅一个topic
		consumer.subscribe(Collections.singleton("second"), new ConsumerRebalanceListener() {
			// 分区再平衡的时候，触发的操作   执行之前操作
			public void onPartitionsRevoked(Collection<TopicPartition> collection) {
				// 提交数据到缓存中
			}

			// 执行之的操作
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				read(offset);

				// 再重置offset
				for (TopicPartition topicPartition : partitions) {
					consumer.seek(topicPartition, offset.get(topicPartition));
				}
			}

		});

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record);
				// 更新我们自定义的offset
				offset.put(new TopicPartition(record.topic(), record.partition()),
						record.offset());

			}
		}

		//consumer.close();

	}

	// 将offset读取到缓存中
	private static void read(Map<TopicPartition, Long> offset) {

	}

	//
	private static void commit(Map<TopicPartition, Long> offset) {

	}
}
