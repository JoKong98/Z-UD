package com.zud.backend.domain.document.service.kafka;

import static org.mockito.BDDMockito.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.zud.backend.common.error.ErrorCode;
import com.zud.backend.common.kafka.KafkaTopics;
import com.zud.backend.domain.consultation.entity.Consultation;
import com.zud.backend.domain.consultation.exception.ConsultationException;
import com.zud.backend.domain.consultation.service.query.ConsultationQueryService;
import com.zud.backend.domain.document.service.notification.OcrNotificationService;

import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
@DisplayName("OcrKafkaFailureHandler 단위 테스트")
class OcrKafkaFailureHandlerTest {

	private static final String CONSULTATION_ID = "consultation-1";
	private static final Long USER_ID = 7L;

	@Mock
	private ConsultationQueryService consultationQueryService;

	@Mock
	private OcrNotificationService ocrNotificationService;

	@Mock
	private Consultation consultation;

	private OcrKafkaFailureHandler failureHandler;

	@BeforeEach
	void setUp() {
		failureHandler = new OcrKafkaFailureHandler(
			JsonMapper.builder().build(), consultationQueryService, ocrNotificationService);
	}

	@Test
	@DisplayName("상담_정보로_사용자를_찾아_OCR_FAILED를_보냄")
	void 상담_정보로_사용자를_찾아_OCR_FAILED를_보냄() {
		// given
		given(consultationQueryService.findByUuid(CONSULTATION_ID)).willReturn(consultation);
		given(consultation.getUserId()).willReturn(USER_ID);

		// when
		failureHandler.onFinalFailure(consumerRecord("{\"consultationId\":\"" + CONSULTATION_ID + "\"}"),
			new IllegalStateException("서류 검증 실패"));

		// then
		then(ocrNotificationService).should().notifyOcrFailed(USER_ID, CONSULTATION_ID);
	}

	@Test
	@DisplayName("상담_정보가_없으면_알림을_보내지_않음")
	void 상담_정보가_없으면_알림을_보내지_않음() {
		// given
		given(consultationQueryService.findByUuid(CONSULTATION_ID))
			.willThrow(new ConsultationException(ErrorCode.CONSULTATION_NOT_FOUND));

		// when
		failureHandler.onFinalFailure(consumerRecord("{\"consultationId\":\"" + CONSULTATION_ID + "\"}"),
			new IllegalStateException("서류 검증 실패"));

		// then
		then(ocrNotificationService).shouldHaveNoInteractions();
	}

	@Test
	@DisplayName("consultationId를_알_수_없으면_아무것도_하지_않음")
	void consultationId를_알_수_없으면_아무것도_하지_않음() {
		// when
		failureHandler.onFinalFailure(consumerRecord("{broken"), new IllegalStateException("역직렬화 실패"));

		// then
		then(consultationQueryService).shouldHaveNoInteractions();
		then(ocrNotificationService).shouldHaveNoInteractions();
	}

	private static ConsumerRecord<String, String> consumerRecord(final String value) {
		return new ConsumerRecord<>(KafkaTopics.OCR_RESPONSE, 0, 0L, "key", value);
	}
}
