package com.zud.backend.common.config;

import static com.zud.backend.common.kafka.KafkaTopics.*;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * DLT 토픽 선언.
 * (Why) 브로커의 토픽 자동 생성이 꺼져 있어(KAFKA_AUTO_CREATE_TOPICS_ENABLE=false) DLT가 없으면 격리 발행이 실패한다.
 * 애플리케이션 시작 시 KafkaAdmin이 없는 토픽만 만든다.
 */
@Configuration
public class KafkaTopicConfig {

	private static final int DLT_PARTITIONS = 3;
	private static final int DLT_REPLICAS = 1;

	@Bean
	public NewTopic reportResponseDeadLetterTopic() {
		return TopicBuilder.name(REPORT_RESPONSE_DLT).partitions(DLT_PARTITIONS).replicas(DLT_REPLICAS).build();
	}

	@Bean
	public NewTopic ocrResponseDeadLetterTopic() {
		return TopicBuilder.name(OCR_RESPONSE_DLT).partitions(DLT_PARTITIONS).replicas(DLT_REPLICAS).build();
	}
}
