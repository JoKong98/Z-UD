package com.zud.backend.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka 컨슈머 공통 에러 핸들러 생성기.
 * (What) 재시도 → DLT 격리 → 도메인 실패 처리 순서로 동작하는 DefaultErrorHandler를 만든다.
 * (Why) 기존에는 재시도 소진 후 메시지가 로그만 남기고 버려져, 결과도 실패도 화면에 전달되지 않았다.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaErrorHandlers {

	/**
	 * DLT 파티션을 지정하지 않는다. 원본 토픽과 DLT의 파티션 수가 달라도 발행이 실패하지 않도록 한다.
	 */
	private static final int ANY_PARTITION = -1;

	public static DefaultErrorHandler create(
		final KafkaOperations<?, ?> deadLetterTemplate,
		final BackOff backOff,
		final KafkaFinalFailureHandler finalFailureHandler
	) {
		DeadLetterPublishingRecoverer deadLetterRecoverer = new DeadLetterPublishingRecoverer(
			deadLetterTemplate,
			(failedRecord, exception) ->
				new TopicPartition(failedRecord.topic() + KafkaTopics.DLT_SUFFIX, ANY_PARTITION)
		);

		ConsumerRecordRecoverer recoverer = (failedRecord, exception) -> {
			// (Why) DLT 발행이 실패하면 예외가 전파되어 메시지가 다시 처리된다. 보존이 확인된 뒤에만 실패 상태를 반영한다.
			deadLetterRecoverer.accept(failedRecord, exception);
			runFinalFailureHandler(finalFailureHandler, failedRecord, exception);
		};

		DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
		errorHandler.addNotRetryableExceptions(InvalidKafkaMessageException.class);
		return errorHandler;
	}

	private static void runFinalFailureHandler(
		final KafkaFinalFailureHandler finalFailureHandler,
		final ConsumerRecord<?, ?> failedRecord,
		final Exception exception
	) {
		try {
			finalFailureHandler.onFinalFailure(failedRecord, exception);
		} catch (RuntimeException ex) {
			// (Why) 메시지는 이미 DLT에 보존됐다. 실패 처리 오류로 원본을 다시 소비하면 DLT에 중복 적재된다.
			log.error("[Kafka] DLT 격리 후 실패 처리 중 오류: topic={}, partition={}, offset={}",
				failedRecord.topic(), failedRecord.partition(), failedRecord.offset(), ex);
		}
	}
}
