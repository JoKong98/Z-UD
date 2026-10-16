package com.zud.backend.common.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * DLT 발행 전용 String 템플릿.
 * (Why) 기본 프로듀서는 JsonSerializer를 쓰므로 원본 String 메시지를 그대로 보내면 따옴표로 한 번 더 감싸진다.
 * DLT에는 원본 바이트를 그대로 보존해야 재처리할 수 있어 StringSerializer를 쓰는 템플릿을 따로 둔다.
 * (Why) KafkaTemplate이나 ProducerFactory를 빈으로 등록하면 Spring Boot가 기본 템플릿을 만들지 않아
 * 기존 프로듀서 주입이 깨진다. 그래서 이 클래스 안에서만 생성하고 수명주기를 직접 관리한다.
 */
@Component
public class DeadLetterKafkaTemplate implements DisposableBean {

	private final DefaultKafkaProducerFactory<String, String> producerFactory;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public DeadLetterKafkaTemplate(final KafkaProperties kafkaProperties) {
		Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		this.producerFactory = new DefaultKafkaProducerFactory<>(props);
		this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
	}

	public KafkaOperations<String, String> operations() {
		return kafkaTemplate;
	}

	@Override
	public void destroy() {
		producerFactory.destroy();
	}
}
