package com.zud.backend.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 재시도를 모두 소진하거나 재시도 불가로 분류된 메시지가 DLT로 격리된 뒤 호출된다.
 * (Why) 메시지를 보존하는 것만으로는 사용자 화면이 실패를 알 수 없으므로,
 * 도메인별로 실패 상태 반영과 실패 알림을 이어서 처리한다.
 */
@FunctionalInterface
public interface KafkaFinalFailureHandler {

	void onFinalFailure(ConsumerRecord<?, ?> failedRecord, Exception exception);
}
