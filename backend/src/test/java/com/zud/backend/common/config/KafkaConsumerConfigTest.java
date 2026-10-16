package com.zud.backend.common.config;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.util.backoff.BackOffExecution;

@DisplayName("KafkaConsumerConfig 재시도 정책 테스트")
class KafkaConsumerConfigTest {

	@Test
	@DisplayName("재시도_간격은_1초_2초_4초_후_중단")
	void 재시도_간격은_1초_2초_4초_후_중단() {
		// given
		BackOffExecution execution = KafkaConsumerConfig.retryBackOff().start();

		// when & then
		assertThat(execution.nextBackOff()).isEqualTo(1_000L);
		assertThat(execution.nextBackOff()).isEqualTo(2_000L);
		assertThat(execution.nextBackOff()).isEqualTo(4_000L);
		assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
	}
}
