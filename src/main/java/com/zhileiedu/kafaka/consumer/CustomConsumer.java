package com.zhileiedu.kafaka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @Author: wzl
 * @Date: 2020/3/4 20:14
 */
public class CustomConsumer {

	private static Map<TopicPartition, Long> currentOffset = new HashMap<TopicPartition, Long>();

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "hadoop01:9092");//Kafka集群
		props.put("group.id", "test");//消费者组，只要group.id相同，就属于同一个消费者组
		props.put("enable.auto.commit", "false");//关闭自动提交offset
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {

			//该方法会在Rebalance之前调用
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				commitOffset(currentOffset);
			}

			//该方法会在Rebalance之后调用
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				currentOffset.clear();
				for (TopicPartition partition : partitions) {
					consumer.seek(partition, getOffset(partition));//定位到最近提交的offset位置继续消费
				}
			}
		});

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
				currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
			}
			commitOffset(currentOffset);
		}
	}

	//获取某分区的最新offset
	private static long getOffset(TopicPartition partition) {
		return 0;
	}

	//提交该消费者所有分区的offset
	private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

	}
}
