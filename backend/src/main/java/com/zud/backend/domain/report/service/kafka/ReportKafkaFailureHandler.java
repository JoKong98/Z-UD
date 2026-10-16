package com.zud.backend.domain.report.service.kafka;

import java.time.LocalDateTime;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import com.zud.backend.common.kafka.KafkaFinalFailureHandler;
import com.zud.backend.common.kafka.KafkaMessages;
import com.zud.backend.domain.report.enums.LoanReportStatus;
import com.zud.backend.domain.report.redis.LoanReportResultCache;
import com.zud.backend.domain.report.repository.ReportRedisRepository;
import com.zud.backend.domain.report.service.notification.ReportNotificationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.json.JsonMapper;

/**
 * 리포트 응답 메시지가 최종 실패했을 때 Redis 상태를 FAILED로 바꾸고 사용자에게 실패를 알린다.
 * (Why) 기존에는 재시도 소진 후 상태가 REQUESTED로 남아 화면이 계속 진행 중으로 보였다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReportKafkaFailureHandler implements KafkaFinalFailureHandler {

	private final JsonMapper jsonMapper;
	private final ReportRedisRepository reportRedisRepository;
	private final ReportNotificationService reportNotificationService;

	@Override
	public void onFinalFailure(final ConsumerRecord<?, ?> failedRecord, final Exception exception) {
		Optional<String> consultationId = KafkaMessages.extractConsultationId(jsonMapper, failedRecord.value());
		if (consultationId.isEmpty()) {
			log.error("[ReportKafka] consultationId를 알 수 없어 실패 상태를 반영하지 못함: offset={}",
				failedRecord.offset());
			return;
		}

		Optional<LoanReportResultCache> requested = reportRedisRepository.findByConsultationId(consultationId.get());
		if (requested.isEmpty()) {
			log.error("[ReportKafka] 리포트 요청 정보가 없어 실패 상태를 반영하지 못함: consultationId={}",
				consultationId.get());
			return;
		}

		LoanReportResultCache cache = requested.get();
		if (cache.status() == LoanReportStatus.COMPLETED) {
			// (Why) 같은 요청의 다른 응답이 이미 완료됐다면 완료 결과를 실패로 덮어쓰지 않는다.
			log.warn("[ReportKafka] 이미 완료된 리포트라 실패 처리를 건너뜀: consultationId={}", cache.consultationId());
			return;
		}

		reportRedisRepository.save(cache.failed(LocalDateTime.now()));
		reportNotificationService.notifyReportFailed(cache.userId(), cache.consultationId());
		log.warn("[ReportKafka] 리포트 최종 실패 처리 완료: consultationId={}, cause={}",
			cache.consultationId(), exception.getMessage());
	}
}
