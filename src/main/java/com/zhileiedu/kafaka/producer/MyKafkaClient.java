package com.zhileiedu.kafaka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author: wzl
 * @Date: 2020/3/4 14:45
 */
public class MyKafkaClient {
	//private static final Logger logger = LoggerFactory.getLogger(MyKafkaClient.class);


	public static void main(String[] args) throws IOException, InterruptedException {

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
		props.put("prefix", "myprefix");

		ArrayList<String> interceptors = new ArrayList<String>();
		interceptors.add("com.zhileiedu.kafaka.interceptor.TimeStampInterceptor");
		interceptors.add("com.zhileiedu.kafaka.interceptor.CountInterceptor");

		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

		// 实例化一个kafka producer 对象
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 5; i++) {
			System.out.println("开始执行发送第" + i + "消息");
			// 返回一个异步的对象,发送消息对topic first里面
			//logger.info("执行了发送...........");
			Thread.sleep(1000);
			kafkaProducer.send(new ProducerRecord<String, String>("second", Integer.toString(i), "value" + i));
		}

		// 关闭kafka 发送者
		kafkaProducer.close();
	}
}
