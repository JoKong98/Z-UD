package com.zud.backend.common.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;

import com.zud.backend.common.kafka.DeadLetterKafkaTemplate;
import com.zud.backend.common.kafka.KafkaErrorHandlers;
import com.zud.backend.domain.document.service.kafka.OcrKafkaFailureHandler;
import com.zud.backend.domain.report.service.kafka.ReportKafkaFailureHandler;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

	/**
	 * (Why) 즉시 재시도는 Redis·네트워크 같은 일시 장애가 회복될 시간을 주지 못한다.
	 * 1초 → 2초 → 4초로 간격을 늘려 3번 재시도한다. 최초 시도를 포함하면 최대 4번 처리한다.
	 */
	private static final int MAX_RETRIES = 3;
	private static final long INITIAL_RETRY_INTERVAL_MS = 1_000L;
	private static final double RETRY_MULTIPLIER = 2.0;
	private static final long MAX_RETRY_INTERVAL_MS = 4_000L;

	private final KafkaProperties kafkaProperties;
	private final DeadLetterKafkaTemplate deadLetterKafkaTemplate;
	private final ReportKafkaFailureHandler reportKafkaFailureHandler;
	private final OcrKafkaFailureHandler ocrKafkaFailureHandler;

	@Bean
	public ConsumerFactory<String, String> loanReportStringConsumerFactory() {
		Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
		props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "loan-report-response-consumer-group");
		props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> loanReportKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(loanReportStringConsumerFactory());
		factory.setCommonErrorHandler(KafkaErrorHandlers.create(
			deadLetterKafkaTemplate.operations(),
			retryBackOff(),
			reportKafkaFailureHandler
		));
		return factory;
	}

	@Bean
	public ConsumerFactory<String, String> ocrStringConsumerFactory() {
		Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
		props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "ocr-response-consumer-group");
		props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> ocrKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(ocrStringConsumerFactory());
		factory.setCommonErrorHandler(KafkaErrorHandlers.create(
			deadLetterKafkaTemplate.operations(),
			retryBackOff(),
			ocrKafkaFailureHandler
		));
		return factory;
	}

	static BackOff retryBackOff() {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
		backOff.setInitialInterval(INITIAL_RETRY_INTERVAL_MS);
		backOff.setMultiplier(RETRY_MULTIPLIER);
		backOff.setMaxInterval(MAX_RETRY_INTERVAL_MS);
		return backOff;
	}
}
