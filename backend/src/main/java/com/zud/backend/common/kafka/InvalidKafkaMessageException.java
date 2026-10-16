package com.zud.backend.common.kafka;

import com.zud.backend.common.error.ErrorCode;
import com.zud.backend.common.error.exception.BusinessException;

/**
 * 메시지 자체가 잘못되어 다시 처리해도 결과가 같은 경우에 던지는 예외.
 * (Why) 역직렬화 실패나 필수 식별자 누락은 재시도해도 회복되지 않으므로,
 * Kafka 에러 핸들러가 이 예외를 재시도 없이 곧바로 DLT로 격리하도록 구분한다.
 */
public class InvalidKafkaMessageException extends BusinessException {

	public InvalidKafkaMessageException(final ErrorCode errorCode) {
		super(errorCode);
	}

	public InvalidKafkaMessageException(final ErrorCode errorCode, final Throwable cause) {
		super(errorCode);
		initCause(cause);
	}
}
