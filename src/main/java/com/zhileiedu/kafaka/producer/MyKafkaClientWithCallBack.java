package com.zhileiedu.kafaka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author: wzl
 * @Date: 2020/3/4 14:45
 */
public class MyKafkaClientWithCallBack {
	//private static final Logger logger = LoggerFactory.getLogger(MyKafkaClient.class);

	public static void main(String[] args) throws IOException {

		Properties props = new Properties();

		// kafka配置
		props.put("bootstrap.servers", "hadoop01:9092");//kafka集群，broker-list
		props.put("acks", "all");
		props.put("retries", 1);//重试次数
		props.put("batch.size", 16384);//批次大小
		props.put("linger.ms", 1);//等待时间
		props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// 实例化一个kafka producer 对象
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
		// 以下发送的都是异步发送
		for (int i = 0; i < 5; i++) {
			System.out.println("开始执行发送第" + i + "消息");
			// 返回一个异步的对象,发送消息对topic second里面
			//logger.info("执行了发送...........");
			kafkaProducer.send(new ProducerRecord<String, String>("second", Integer.toString(i), "value" + i), new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// 数据发送成后的回调
					System.out.println("第" + recordMetadata.partition() + "分区返回数据 ," + "offset=" + recordMetadata.offset());
				}
			});
		}

		// 关闭kafka 发送者
		kafkaProducer.close();
	}
}
