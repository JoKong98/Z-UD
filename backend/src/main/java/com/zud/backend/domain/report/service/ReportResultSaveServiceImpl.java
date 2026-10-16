package com.zud.backend.domain.report.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zud.backend.common.error.ErrorCode;
import com.zud.backend.common.kafka.InvalidKafkaMessageException;
import com.zud.backend.domain.report.dto.message.LoanReportResMessage;
import com.zud.backend.domain.report.exception.ReportException;
import com.zud.backend.domain.report.redis.LoanReportResultCache;
import com.zud.backend.domain.report.repository.ReportRedisRepository;
import com.zud.backend.domain.report.service.notification.ReportNotificationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ReportResultSaveServiceImpl implements ReportResultSaveService {
	private static final BigDecimal ONE = BigDecimal.ONE;
	private static final BigDecimal REPAYMENT_PERIOD_YEARS = BigDecimal.valueOf(30);
	private static final BigDecimal STRESS_RATE_ADJUSTMENT = BigDecimal.valueOf(0.006); // 0.6%

	private final ReportRedisRepository reportRedisRepository;
	private final ObjectMapper objectMapper;
	private final ReportNotificationService reportNotificationService;

	@Override
	@Transactional
	public void saveReportResult(String messageBody) {
		log.info("[Report] Kafka 응답 처리 시작");
		// (Why) 메시지 자체가 잘못된 경우는 재시도해도 결과가 같으므로 처리 전에 걸러 곧바로 DLT로 보낸다.
		JsonNode root = parseTree(messageBody);
		String consultationId = extractConsultationId(root);
		LoanReportResMessage message = parseMessage(messageBody);
		log.info("[Report] Kafka 응답 consultationId 파싱 완료: consultationId={}", consultationId);

		try {
			LoanReportResultCache existing = reportRedisRepository.findByConsultationId(consultationId)
				.orElseThrow(() -> new ReportException(ErrorCode.REPORT_REQUEST_NOT_FOUND));

			String payloadToStore = buildPayloadToStore(root, messageBody);

			reportRedisRepository.save(
				existing.completed(payloadToStore, message.completedAt())
			);
			reportNotificationService.notifyReportCompleted(existing.userId(), consultationId);
			log.info("[Report] 리포트 결과 저장/알림 완료: userId={}, consultationId={}", existing.userId(), consultationId);
		} catch (ReportException ex) {
			log.warn("[Report] 리포트 결과 처리 중 도메인 예외: {}", ex.getErrorCode());
			throw ex;
		} catch (Exception ex) {
			log.error("[Report] 리포트 결과 처리 실패", ex);
			throw new ReportException(ErrorCode.REPORT_RESULT_PROCESSING_FAILED);
		}

	}

	private JsonNode parseTree(String messageBody) {
		if (messageBody == null || messageBody.isBlank()) {
			throw new InvalidKafkaMessageException(ErrorCode.DESERIALIZE_ERROR);
		}
		try {
			return objectMapper.readTree(messageBody);
		} catch (JsonProcessingException ex) {
			log.warn("[Report] Kafka 응답 JSON 파싱 실패");
			throw new InvalidKafkaMessageException(ErrorCode.DESERIALIZE_ERROR, ex);
		}
	}

	private String extractConsultationId(JsonNode root) {
		JsonNode consultationIdNode = root.path("consultationId");
		if (consultationIdNode.isMissingNode() || consultationIdNode.isNull()) {
			log.warn("[Report] Kafka 응답에 consultationId 없음");
			throw new InvalidKafkaMessageException(ErrorCode.REPORT_UUID_NOT_FOUND);
		}
		return consultationIdNode.asText();
	}

	private LoanReportResMessage parseMessage(String messageBody) {
		try {
			return objectMapper.readValue(messageBody, LoanReportResMessage.class);
		} catch (JsonProcessingException ex) {
			log.warn("[Report] Kafka 응답 메시지 매핑 실패");
			throw new InvalidKafkaMessageException(ErrorCode.DESERIALIZE_ERROR, ex);
		}
	}

	private String buildPayloadToStore(JsonNode root, String messageBody) {
		JsonNode resultNode = root.path("result");
		if (resultNode.isMissingNode() || resultNode.isNull()) {
			return messageBody;
		}

		if (!resultNode.isObject()) {
			return resultNode.toString();
		}

		ObjectNode enrichedResultNode = resultNode.deepCopy();
		enrichLtvBasedLoanLimit(enrichedResultNode);
		enrichDsrBasedLoanLimit(enrichedResultNode);
		return enrichedResultNode.toString();
	}

	private void enrichLtvBasedLoanLimit(ObjectNode resultNode) {
		for (Map.Entry<String, JsonNode> productEntry : resultNode.properties()) {
			JsonNode productNode = productEntry.getValue();
			if (!productNode.isObject()) {
				continue;
			}

			ObjectNode productObject = (ObjectNode)productNode;
			BigDecimal collateralMarketPrice = extractNumber(productObject.path("collateralMarketPrice").path("value"));
			JsonNode ltvRatioSourceNode = productObject.path("LTVRatio").path("value");
			BigDecimal ltvRatioRaw = extractPercentNumber(ltvRatioSourceNode);
			BigDecimal ltvRatioForCalculation = normalizeLtvRatio(ltvRatioRaw);
			BigDecimal maximumClaimAmount = sumMaximumClaimAmount(productObject.path("seniorRights").path("value"));
			BigDecimal totalRemainingLoanBalance = extractNumber(
				productObject.path("totalRemainingLoanBalance").path("value"));

			ObjectNode ltvBasedLoanLimitNode = objectMapper.createObjectNode();
			putDecimalOrNull(ltvBasedLoanLimitNode, "collateralMarketPrice", collateralMarketPrice);
			putNodeOrNull(ltvBasedLoanLimitNode, "LTVRatio", ltvRatioSourceNode);
			putDecimalOrNull(ltvBasedLoanLimitNode, "maximumClaimAmount", maximumClaimAmount);
			putDecimalOrNull(ltvBasedLoanLimitNode, "totalRemainingLoanBalance", totalRemainingLoanBalance);

			if (collateralMarketPrice == null || ltvRatioForCalculation == null || maximumClaimAmount == null
				|| totalRemainingLoanBalance == null) {
				ltvBasedLoanLimitNode.putNull("value");
				productObject.set("ltvBasedLoanLimit", ltvBasedLoanLimitNode);
				continue;
			}

			BigDecimal ltvBasedLoanLimit = collateralMarketPrice
				.multiply(ltvRatioForCalculation)
				.subtract(maximumClaimAmount)
				.subtract(totalRemainingLoanBalance);

			putRoundedIntegerOrNull(ltvBasedLoanLimitNode, "value", ltvBasedLoanLimit);
			productObject.set("ltvBasedLoanLimit", ltvBasedLoanLimitNode);
		}
	}

	private void putDecimalOrNull(ObjectNode target, String key, BigDecimal value) {
		if (value == null) {
			target.putNull(key);
			return;
		}
		target.put(key, value);
	}

	private void putRoundedIntegerOrNull(ObjectNode target, String key, BigDecimal value) {
		if (value == null) {
			target.putNull(key);
			return;
		}
		target.put(key, value.setScale(0, RoundingMode.HALF_UP));
	}

	private void putNodeOrNull(ObjectNode target, String key, JsonNode source) {
		if (source == null || source.isMissingNode() || source.isNull()) {
			target.putNull(key);
			return;
		}
		target.set(key, source.deepCopy());
	}

	private void enrichDsrBasedLoanLimit(ObjectNode resultNode) {
		for (Map.Entry<String, JsonNode> productEntry : resultNode.properties()) {
			JsonNode productNode = productEntry.getValue();
			if (!productNode.isObject()) {
				continue;
			}

			ObjectNode productObject = (ObjectNode)productNode;
			JsonNode dsrRatioSourceNode = productObject.path("DSRRatio").path("value");
			BigDecimal dsrRatioRaw = extractPercentNumber(dsrRatioSourceNode);
			BigDecimal dsrRatioForCalculation = normalizeLtvRatio(dsrRatioRaw);
			BigDecimal annualIncomeTotal = extractAnnualIncomeTotal(productObject);
			BigDecimal annualPrincipalAndInterestRepayment = extractNumber(
				productObject.path("annualPrincipalAndInterestRepayment").path("value")
			);
			JsonNode interestRateSourceNode = productObject.path("interestRate");
			BigDecimal interestRateRaw = extractPercentNumber(interestRateSourceNode);
			BigDecimal interestRateForCalculation = normalizeLtvRatio(interestRateRaw);
			JsonNode stressDsrSourceNode = productObject.path("stressDSR").path("value");
			BigDecimal stressDsrRaw = extractPercentNumber(stressDsrSourceNode);
			BigDecimal stressDsrForCalculation = normalizeLtvRatio(stressDsrRaw);

			ObjectNode dsrBasedLoanLimitNode = objectMapper.createObjectNode();
			putNodeOrNull(dsrBasedLoanLimitNode, "DSRRatio", dsrRatioSourceNode);
			putDecimalOrNull(dsrBasedLoanLimitNode, "annualIncomeTotal", annualIncomeTotal);
			putDecimalOrNull(
				dsrBasedLoanLimitNode,
				"annualPrincipalAndInterestRepayment",
				annualPrincipalAndInterestRepayment
			);
			putNodeOrNull(dsrBasedLoanLimitNode, "interestRate", interestRateSourceNode);
			dsrBasedLoanLimitNode.put("stressRateAdjustment", "0.6%");
			putNodeOrNull(dsrBasedLoanLimitNode, "stressDSR", stressDsrSourceNode);
			putDecimalOrNull(dsrBasedLoanLimitNode, "repaymentPeriodYears", REPAYMENT_PERIOD_YEARS);

			if (dsrRatioForCalculation == null || annualIncomeTotal == null
				|| annualPrincipalAndInterestRepayment == null
				|| interestRateForCalculation == null || stressDsrForCalculation == null) {
				dsrBasedLoanLimitNode.putNull("value");
				productObject.set("dsrBasedLoanLimit", dsrBasedLoanLimitNode);
				continue;
			}

			BigDecimal numerator = dsrRatioForCalculation.multiply(annualIncomeTotal)
				.subtract(annualPrincipalAndInterestRepayment);
			BigDecimal denominator = ONE.add(
				interestRateForCalculation
					.add(STRESS_RATE_ADJUSTMENT)
					.add(stressDsrForCalculation)
					.multiply(REPAYMENT_PERIOD_YEARS)
			);
			BigDecimal dsrBasedLoanLimit = numerator
				.multiply(REPAYMENT_PERIOD_YEARS)
				.divide(denominator, 10, RoundingMode.HALF_UP);

			putRoundedIntegerOrNull(dsrBasedLoanLimitNode, "value", dsrBasedLoanLimit);
			productObject.set("dsrBasedLoanLimit", dsrBasedLoanLimitNode);
		}
	}

	private BigDecimal extractAnnualIncomeTotal(ObjectNode productObject) {
		BigDecimal annualIncomeTotal = extractNumber(productObject.path("annualIncomeTotal").path("value"));
		if (annualIncomeTotal != null) {
			return annualIncomeTotal;
		}
		// 개인사업자 응답 대응: annualIncomeTotal이 없으면 incomeAmount를 fallback으로 사용
		return extractNumber(productObject.path("incomeAmount").path("value"));
	}

	private BigDecimal sumMaximumClaimAmount(JsonNode seniorRightsValue) {
		if (seniorRightsValue == null || seniorRightsValue.isNull() || !seniorRightsValue.isArray()) {
			return null;
		}

		BigDecimal total = BigDecimal.ZERO;
		for (JsonNode right : seniorRightsValue) {
			BigDecimal amount = extractNumber(right.path("maximumClaimAmount"));
			if (amount == null) {
				return null;
			}
			total = total.add(amount);
		}
		return total;
	}

	private BigDecimal normalizeLtvRatio(BigDecimal rawLtvRatio) {
		if (rawLtvRatio == null) {
			return null;
		}
		return rawLtvRatio.compareTo(BigDecimal.ONE) > 0
			? rawLtvRatio.divide(BigDecimal.valueOf(100))
			: rawLtvRatio;
	}

	private BigDecimal extractNumber(JsonNode node) {
		if (node == null || node.isMissingNode() || node.isNull()) {
			return null;
		}

		if (node.isNumber()) {
			return node.decimalValue();
		}

		if (node.isTextual()) {
			String normalized = node.asText()
				.replace(",", "")
				.trim();
			if (normalized.isEmpty()) {
				return null;
			}
			try {
				return new BigDecimal(normalized);
			} catch (NumberFormatException _) {
				return null;
			}
		}

		return null;
	}

	private BigDecimal extractPercentNumber(JsonNode node) {
		if (node == null || node.isMissingNode() || node.isNull()) {
			return null;
		}

		if (node.isNumber()) {
			return node.decimalValue();
		}

		if (node.isTextual()) {
			String normalized = node.asText()
				.replace("%", "")
				.replace(",", "")
				.trim();
			if (normalized.isEmpty()) {
				return null;
			}
			try {
				return new BigDecimal(normalized);
			} catch (NumberFormatException _) {
				return null;
			}
		}

		return null;
	}
}
