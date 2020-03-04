package com.zhileiedu.kafaka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: wzl
 * @Date: 2020/3/4 20:41
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {
	private int success;
	private int fail;

	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
		return producerRecord;
	}

	public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
		if (e == null) {
			success++;
		} else {
			fail++;
		}
	}

	public void close() {
		System.out.println("成功数据=" + success);
		System.out.println("失败数据=" + fail);
	}

	public void configure(Map<String, ?> map) {

	}
}
