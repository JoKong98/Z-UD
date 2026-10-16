package com.zud.backend.common.kafka;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

import com.zud.backend.common.error.ErrorCode;

/**
 * 재시도 → DLT 격리 → 실패 처리 흐름을 실제 브로커(EmbeddedKafka)로 검증한다.
 * 운영 백오프(1초·2초·4초)는 KafkaConsumerConfigTest에서 따로 검증하고, 여기서는 10ms 간격으로 같은 횟수를 재시도한다.
 */
@EmbeddedKafka(partitions = 1, topics = {
	KafkaErrorHandlersTest.INVALID_TOPIC,
	KafkaErrorHandlersTest.INVALID_TOPIC + KafkaTopics.DLT_SUFFIX,
	KafkaErrorHandlersTest.TRANSIENT_TOPIC,
	KafkaErrorHandlersTest.TRANSIENT_TOPIC + KafkaTopics.DLT_SUFFIX,
	KafkaErrorHandlersTest.RECOVER_TOPIC,
	KafkaErrorHandlersTest.RECOVER_TOPIC + KafkaTopics.DLT_SUFFIX,
	KafkaErrorHandlersTest.HANDLER_ERROR_TOPIC,
	KafkaErrorHandlersTest.HANDLER_ERROR_TOPIC + KafkaTopics.DLT_SUFFIX
})
@DisplayName("KafkaErrorHandlers 통합 테스트")
class KafkaErrorHandlersTest {

	static final String INVALID_TOPIC = "invalid-message-topic";
	static final String TRANSIENT_TOPIC = "transient-failure-topic";
	static final String RECOVER_TOPIC = "recover-on-retry-topic";
	static final String HANDLER_ERROR_TOPIC = "handler-error-topic";

	private static final int MAX_RETRIES = 3;
	private static final long TEST_RETRY_INTERVAL_MS = 10L;
	private static final Duration WAIT = Duration.ofSeconds(15);

	private EmbeddedKafkaBroker broker;
	private DefaultKafkaProducerFactory<String, String> producerFactory;
	private KafkaTemplate<String, String> kafkaTemplate;
	private final List<KafkaMessageListenerContainer<String, String>> containers = new ArrayList<>();
	private final BlockingQueue<ConsumerRecord<?, ?>> finalFailures = new LinkedBlockingQueue<>();

	@BeforeEach
	void setUp(final EmbeddedKafkaBroker embeddedKafkaBroker) {
		this.broker = embeddedKafkaBroker;
		this.producerFactory = new DefaultKafkaProducerFactory<>(
			KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer());
		this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
	}

	@AfterEach
	void tearDown() {
		containers.forEach(KafkaMessageListenerContainer::stop);
		producerFactory.destroy();
	}

	@Test
	@DisplayName("잘못된_메시지는_재시도_없이_DLT로_격리되고_실패_처리가_호출됨")
	void 잘못된_메시지는_재시도_없이_DLT로_격리되고_실패_처리가_호출됨() throws Exception {
		// given
		AtomicInteger attempts = new AtomicInteger();
		startContainer(INVALID_TOPIC, data -> {
			attempts.incrementAndGet();
			throw new InvalidKafkaMessageException(ErrorCode.DESERIALIZE_ERROR);
		}, (failedRecord, exception) -> finalFailures.add(failedRecord));

		// when
		kafkaTemplate.send(INVALID_TOPIC, "key", "{broken").get();

		// then
		ConsumerRecord<?, ?> failed = finalFailures.poll(WAIT.toSeconds(), TimeUnit.SECONDS);
		assertThat(failed).isNotNull();
		assertThat(failed.value()).isEqualTo("{broken");
		assertThat(attempts.get()).isEqualTo(1);

		ConsumerRecord<String, String> deadLetter = readSingleDeadLetter(INVALID_TOPIC);
		assertThat(deadLetter.value()).isEqualTo("{broken");
		assertThat(header(deadLetter, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(INVALID_TOPIC);
	}

	@Test
	@DisplayName("일시_장애는_최초_1회와_재시도_3회_후_DLT로_격리됨")
	void 일시_장애는_최초_1회와_재시도_3회_후_DLT로_격리됨() throws Exception {
		// given
		AtomicInteger attempts = new AtomicInteger();
		startContainer(TRANSIENT_TOPIC, data -> {
			attempts.incrementAndGet();
			throw new IllegalStateException("Redis 연결 실패");
		}, (failedRecord, exception) -> finalFailures.add(failedRecord));

		// when
		kafkaTemplate.send(TRANSIENT_TOPIC, "key", "{\"consultationId\":\"c-1\"}").get();

		// then
		ConsumerRecord<?, ?> failed = finalFailures.poll(WAIT.toSeconds(), TimeUnit.SECONDS);
		assertThat(failed).isNotNull();
		assertThat(attempts.get()).isEqualTo(1 + MAX_RETRIES);
		assertThat(readSingleDeadLetter(TRANSIENT_TOPIC).value()).isEqualTo("{\"consultationId\":\"c-1\"}");
	}

	@Test
	@DisplayName("재시도_중_회복되면_DLT로_보내지_않고_실패_처리도_호출하지_않음")
	void 재시도_중_회복되면_DLT로_보내지_않고_실패_처리도_호출하지_않음() throws Exception {
		// given
		AtomicInteger attempts = new AtomicInteger();
		CountDownLatch processed = new CountDownLatch(1);
		startContainer(RECOVER_TOPIC, data -> {
			if (attempts.incrementAndGet() < 3) {
				throw new IllegalStateException("일시 장애");
			}
			processed.countDown();
		}, (failedRecord, exception) -> finalFailures.add(failedRecord));

		// when
		kafkaTemplate.send(RECOVER_TOPIC, "key", "{\"consultationId\":\"c-2\"}").get();

		// then
		assertThat(processed.await(WAIT.toSeconds(), TimeUnit.SECONDS)).isTrue();
		assertThat(attempts.get()).isEqualTo(3);
		assertThat(finalFailures.poll(1, TimeUnit.SECONDS)).isNull();
		assertThat(countDeadLetters(RECOVER_TOPIC)).isZero();
	}

	@Test
	@DisplayName("실패_처리에서_예외가_나도_메시지를_다시_소비하지_않아_DLT에_한_번만_적재됨")
	void 실패_처리에서_예외가_나도_메시지를_다시_소비하지_않아_DLT에_한_번만_적재됨() throws Exception {
		// given
		AtomicInteger attempts = new AtomicInteger();
		CountDownLatch handlerCalled = new CountDownLatch(1);
		startContainer(HANDLER_ERROR_TOPIC, data -> {
			attempts.incrementAndGet();
			throw new InvalidKafkaMessageException(ErrorCode.DESERIALIZE_ERROR);
		}, (failedRecord, exception) -> {
			handlerCalled.countDown();
			throw new IllegalStateException("알림 서버 장애");
		});

		// when
		kafkaTemplate.send(HANDLER_ERROR_TOPIC, "key", "{broken").get();

		// then
		assertThat(handlerCalled.await(WAIT.toSeconds(), TimeUnit.SECONDS)).isTrue();
		assertThat(countDeadLetters(HANDLER_ERROR_TOPIC)).isEqualTo(1);
		assertThat(attempts.get()).isEqualTo(1);
	}

	private void startContainer(
		final String topic,
		final MessageListener<String, String> listener,
		final KafkaFinalFailureHandler finalFailureHandler
	) {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(broker, topic + "-group", false);
		DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
			consumerProps, new StringDeserializer(), new StringDeserializer());

		ContainerProperties containerProperties = new ContainerProperties(topic);
		containerProperties.setMessageListener(listener);

		KafkaMessageListenerContainer<String, String> container =
			new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
		container.setCommonErrorHandler(KafkaErrorHandlers.create(
			kafkaTemplate,
			new FixedBackOff(TEST_RETRY_INTERVAL_MS, MAX_RETRIES),
			finalFailureHandler
		));
		container.start();
		ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());
		containers.add(container);
	}

	private ConsumerRecord<String, String> readSingleDeadLetter(final String topic) {
		String deadLetterTopic = topic + KafkaTopics.DLT_SUFFIX;
		try (Consumer<String, String> consumer = deadLetterConsumer(topic)) {
			broker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);
			return KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic, WAIT);
		}
	}

	/**
	 * DLT를 끝까지 읽어 적재된 메시지 수를 센다. 늦게 들어오는 중복까지 잡기 위해 빈 poll이 나올 때까지 읽는다.
	 */
	private int countDeadLetters(final String topic) {
		String deadLetterTopic = topic + KafkaTopics.DLT_SUFFIX;
		try (Consumer<String, String> consumer = deadLetterConsumer(topic)) {
			broker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);
			int count = 0;
			int records;
			do {
				records = consumer.poll(Duration.ofSeconds(2)).count();
				count += records;
			} while (records > 0);
			return count;
		}
	}

	private Consumer<String, String> deadLetterConsumer(final String topic) {
		Map<String, Object> props = KafkaTestUtils.consumerProps(broker, topic + "-dlt-reader", false);
		return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
			.createConsumer();
	}

	private static String header(final ConsumerRecord<String, String> consumerRecord, final String key) {
		return new String(consumerRecord.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
	}
}
