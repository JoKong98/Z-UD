package com.zud.backend.domain.document.service.kafka;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import com.zud.backend.common.error.exception.BusinessException;
import com.zud.backend.common.kafka.KafkaFinalFailureHandler;
import com.zud.backend.common.kafka.KafkaMessages;
import com.zud.backend.domain.consultation.service.query.ConsultationQueryService;
import com.zud.backend.domain.document.service.notification.OcrNotificationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.json.JsonMapper;

/**
 * OCR 응답 메시지가 최종 실패했을 때 사용자에게 OCR_FAILED를 보낸다.
 * (Why) 프론트는 OCR_FAILED를 받으면 OCR 단계를 오류로 표시하도록 구현돼 있었지만,
 * 백엔드에서 이 이벤트를 보내는 곳이 없어 실패해도 화면은 계속 분석 중으로 남았다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OcrKafkaFailureHandler implements KafkaFinalFailureHandler {

	private final JsonMapper jsonMapper;
	private final ConsultationQueryService consultationQueryService;
	private final OcrNotificationService ocrNotificationService;

	@Override
	public void onFinalFailure(final ConsumerRecord<?, ?> failedRecord, final Exception exception) {
		Optional<String> consultationId = KafkaMessages.extractConsultationId(jsonMapper, failedRecord.value());
		if (consultationId.isEmpty()) {
			log.error("[OcrKafka] consultationId를 알 수 없어 실패 알림을 보내지 못함: offset={}",
				failedRecord.offset());
			return;
		}

		Long userId;
		try {
			userId = consultationQueryService.findByUuid(consultationId.get()).getUserId();
		} catch (BusinessException ex) {
			log.error("[OcrKafka] 상담 정보가 없어 실패 알림을 보내지 못함: consultationId={}",
				consultationId.get(), ex);
			return;
		}

		ocrNotificationService.notifyOcrFailed(userId, consultationId.get());
		log.warn("[OcrKafka] OCR 최종 실패 알림 발송: consultationId={}, cause={}",
			consultationId.get(), exception.getMessage());
	}
}
