package com.zud.backend.domain.document.service.kafka;

import static com.zud.backend.common.kafka.KafkaTopics.*;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import com.zud.backend.common.error.ErrorCode;
import com.zud.backend.common.kafka.InvalidKafkaMessageException;
import com.zud.backend.domain.consultation.service.query.ConsultationQueryService;
import com.zud.backend.domain.document.dto.request.DocumentExtractionReqDto;
import com.zud.backend.domain.document.dto.response.DocumentExtractionResDto;
import com.zud.backend.domain.document.exception.DocumentException;
import com.zud.backend.domain.document.redis.OcrExtractionResultCache;
import com.zud.backend.domain.document.repository.OcrResultRedisRepository;
import com.zud.backend.domain.document.service.facade.DocumentFacadeService;
import com.zud.backend.domain.document.service.notification.OcrNotificationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class OcrKafkaConsumer {

	private final JsonMapper objectMapper;
	private final ConsultationQueryService consultationQueryService;
	private final DocumentFacadeService documentFacadeService;
	private final OcrResultRedisRepository ocrResultRedisRepository;
	private final OcrNotificationService ocrNotificationService;

	@KafkaListener(topics = OCR_RESPONSE, containerFactory = "ocrKafkaListenerContainerFactory")
	public void consume(final String messageBody) {
		// (Why) 역직렬화 실패는 재시도해도 회복되지 않으므로 처리 로직과 분리해 곧바로 DLT로 보낸다.
		DocumentExtractionReqDto reqDto = parse(messageBody);
		try {
			DocumentExtractionResDto result = documentFacadeService.validateDocuments(reqDto);
			ocrResultRedisRepository.save(OcrExtractionResultCache.of(reqDto.consultationId(), result));
			Long userId = consultationQueryService.findByUuid(reqDto.consultationId()).getUserId();
			ocrNotificationService.notifyOcrCompleted(userId, reqDto.consultationId());
		} catch (DocumentException e) {
			log.error("[OCR] Kafka 응답 처리 실패", e);
			throw e;
		} catch (Exception e) {
			log.error("[OCR] Kafka 응답 처리 중 예외 발생", e);
			throw new DocumentException(ErrorCode.INTERNAL_SERVER_ERROR);
		}
	}

	private DocumentExtractionReqDto parse(final String messageBody) {
		if (messageBody == null || messageBody.isBlank()) {
			throw new InvalidKafkaMessageException(ErrorCode.DESERIALIZE_ERROR);
		}
		try {
			return objectMapper.readValue(messageBody, DocumentExtractionReqDto.class);
		} catch (JacksonException e) {
			log.error("[OCR] Kafka 응답 역직렬화 실패", e);
			throw new InvalidKafkaMessageException(ErrorCode.DESERIALIZE_ERROR, e);
		}
	}
}
