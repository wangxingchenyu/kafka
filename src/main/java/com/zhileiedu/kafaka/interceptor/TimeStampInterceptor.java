package com.zhileiedu.kafaka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: wzl
 * @Date: 2020/3/4 20:13
 */
public class TimeStampInterceptor implements ProducerInterceptor<String, String> {

	private String prefix;

	// 发送的时候调用
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

		return new ProducerRecord<String, String>(record.topic(),
				record.partition(),
				record.timestamp(),
				record.key(),
				prefix + System.currentTimeMillis() + record.value(),
				record.headers()
		);
	}

	// 收到返回的消息后调用
	public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

	}

	/**
	 * producer 关闭的时候执行
	 */
	public void close() {

	}

	// 配置文件
	public void configure(Map<String, ?> map) {
		// 从配置的文件中读出来
		prefix = (String) map.get("prefix");
	}
}
