package com.zud.backend.domain.report.service.notification;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.zud.backend.domain.notification.dto.NotificationResDto;
import com.zud.backend.domain.notification.service.NotificationFacadeService;
import com.zud.backend.domain.report.enums.ReportEventType;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class ReportNotificationService {

	private final NotificationFacadeService notificationFacadeService;

	public void notifyReportCompleted(final Long userId, final String consultationId) {
		send(userId, ReportEventType.REPORT_COMPLETED, Map.of("consultationId", consultationId));
	}

	public void notifyReportFailed(final Long userId, final String consultationId) {
		send(userId, ReportEventType.REPORT_FAILED, Map.of("consultationId", consultationId));
	}

	private void send(final Long userId, final ReportEventType eventType, final Object data) {
		try {
			notificationFacadeService.send(
				userId,
				new NotificationResDto(eventType.name(), eventType.defaultMessage(), data)
			);
		} catch (Exception ex) {
			log.warn("[ReportSSE] 알림 전송 실패: userId={}, eventType={}, error={}",
				userId, eventType.name(), ex.getMessage());
		}
	}
}
