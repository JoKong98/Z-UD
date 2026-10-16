package com.zud.backend.domain.report.enums;

public enum ReportEventType {
	REPORT_COMPLETED("리포트 생성이 완료되었습니다."),
	REPORT_FAILED("리포트 생성에 실패했습니다.");

	private final String defaultMessage;

	ReportEventType(final String defaultMessage) {
		this.defaultMessage = defaultMessage;
	}

	public String defaultMessage() {
		return defaultMessage;
	}
}
