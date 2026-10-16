package com.zud.backend.domain.report.service.kafka;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

import java.time.LocalDateTime;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.zud.backend.common.kafka.KafkaTopics;
import com.zud.backend.domain.report.enums.LoanReportStatus;
import com.zud.backend.domain.report.redis.LoanReportResultCache;
import com.zud.backend.domain.report.repository.ReportRedisRepository;
import com.zud.backend.domain.report.service.notification.ReportNotificationService;

import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
@DisplayName("ReportKafkaFailureHandler 단위 테스트")
class ReportKafkaFailureHandlerTest {

	private static final String CONSULTATION_ID = "consultation-1";
	private static final Long USER_ID = 7L;

	@Mock
	private ReportRedisRepository reportRedisRepository;

	@Mock
	private ReportNotificationService reportNotificationService;

	private ReportKafkaFailureHandler failureHandler;

	@BeforeEach
	void setUp() {
		failureHandler = new ReportKafkaFailureHandler(
			JsonMapper.builder().build(), reportRedisRepository, reportNotificationService);
	}

	@Test
	@DisplayName("요청_상태의_리포트는_FAILED로_바꾸고_실패_알림을_보냄")
	void 요청_상태의_리포트는_FAILED로_바꾸고_실패_알림을_보냄() {
		// given
		LoanReportResultCache requested =
			LoanReportResultCache.requested(CONSULTATION_ID, USER_ID, LocalDateTime.now());
		given(reportRedisRepository.findByConsultationId(CONSULTATION_ID)).willReturn(Optional.of(requested));

		// when
		failureHandler.onFinalFailure(consumerRecord("{\"consultationId\":\"" + CONSULTATION_ID + "\"}"),
			new IllegalStateException("Redis 연결 실패"));

		// then
		ArgumentCaptor<LoanReportResultCache> saved = ArgumentCaptor.forClass(LoanReportResultCache.class);
		then(reportRedisRepository).should().save(saved.capture());
		assertThat(saved.getValue().status()).isEqualTo(LoanReportStatus.FAILED);
		assertThat(saved.getValue().completedAt()).isNotNull();
		then(reportNotificationService).should().notifyReportFailed(USER_ID, CONSULTATION_ID);
	}

	@Test
	@DisplayName("이미_완료된_리포트는_실패로_덮어쓰지_않음")
	void 이미_완료된_리포트는_실패로_덮어쓰지_않음() {
		// given
		LoanReportResultCache completed = LoanReportResultCache
			.requested(CONSULTATION_ID, USER_ID, LocalDateTime.now())
			.completed("{}", LocalDateTime.now());
		given(reportRedisRepository.findByConsultationId(CONSULTATION_ID)).willReturn(Optional.of(completed));

		// when
		failureHandler.onFinalFailure(consumerRecord("{\"consultationId\":\"" + CONSULTATION_ID + "\"}"),
			new IllegalStateException("중복 응답 처리 실패"));

		// then
		then(reportRedisRepository).should(never()).save(any());
		then(reportNotificationService).shouldHaveNoInteractions();
	}

	@Test
	@DisplayName("consultationId를_알_수_없으면_상태와_알림을_건드리지_않음")
	void consultationId를_알_수_없으면_상태와_알림을_건드리지_않음() {
		// when
		failureHandler.onFinalFailure(consumerRecord("{broken"), new IllegalStateException("역직렬화 실패"));

		// then
		then(reportRedisRepository).shouldHaveNoInteractions();
		then(reportNotificationService).shouldHaveNoInteractions();
	}

	@Test
	@DisplayName("요청_정보가_없으면_알림을_보내지_않음")
	void 요청_정보가_없으면_알림을_보내지_않음() {
		// given
		given(reportRedisRepository.findByConsultationId(CONSULTATION_ID)).willReturn(Optional.empty());

		// when
		failureHandler.onFinalFailure(consumerRecord("{\"consultationId\":\"" + CONSULTATION_ID + "\"}"),
			new IllegalStateException("요청 정보 없음"));

		// then
		then(reportRedisRepository).should(never()).save(any());
		then(reportNotificationService).shouldHaveNoInteractions();
	}

	private static ConsumerRecord<String, String> consumerRecord(final String value) {
		return new ConsumerRecord<>(KafkaTopics.REPORT_RESPONSE, 0, 0L, "key", value);
	}
}
