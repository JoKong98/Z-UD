package com.zud.backend.common.kafka;

import java.util.Optional;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaMessages {

	private static final String CONSULTATION_ID_FIELD = "consultationId";

	/**
	 * 실패한 메시지에서 consultationId를 최대한 꺼낸다.
	 * (Why) 실패 알림 대상(사용자)을 찾는 유일한 단서다. 메시지가 깨졌어도 예외 없이 빈 값을 돌려준다.
	 */
	public static Optional<String> extractConsultationId(final JsonMapper jsonMapper, final Object messageValue) {
		if (!(messageValue instanceof String messageBody) || messageBody.isBlank()) {
			return Optional.empty();
		}

		try {
			JsonNode idNode = jsonMapper.readTree(messageBody).path(CONSULTATION_ID_FIELD);
			if (idNode.isMissingNode() || idNode.isNull()) {
				return Optional.empty();
			}
			String consultationId = idNode.asString();
			return consultationId.isBlank() ? Optional.empty() : Optional.of(consultationId);
		} catch (JacksonException ex) {
			return Optional.empty();
		}
	}
}
