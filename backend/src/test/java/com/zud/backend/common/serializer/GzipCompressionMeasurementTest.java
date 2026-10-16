package com.zud.backend.common.serializer;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실제 Redis 저장 데이터와 같은 형태의 JSON으로 GZIP 압축률을 잰다.
 * src/test/resources/gzip-samples/ 에 *.json 파일을 넣으면 실행되고, 없으면 건너뛴다.
 * 수집 방법은 같은 폴더의 README.md 참고.
 */
@DisplayName("GZIP 압축률 실측")
class GzipCompressionMeasurementTest {

	private static final Path SAMPLE_DIR = Path.of("src/test/resources/gzip-samples");

	private static final ObjectMapper REDIS_OBJECT_MAPPER = JsonMapper.builder()
		.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
		.build();

	@Test
	@DisplayName("샘플별_원본_크기와_저장_크기를_비교함")
	void 샘플별_원본_크기와_저장_크기를_비교함() throws IOException {
		List<Path> samples = findSamples();
		assumeFalse(samples.isEmpty(), "gzip-samples 폴더에 JSON 샘플이 없어 실측을 건너뜁니다.");

		GzipRedisSerializer<JsonNode> serializer =
			new GzipRedisSerializer<>(REDIS_OBJECT_MAPPER, new TypeReference<>() {
			});

		long totalRaw = 0;
		long totalStored = 0;
		System.out.printf("%-40s %10s %10s %8s%n", "sample", "raw(B)", "stored(B)", "saved");

		for (Path sample : samples) {
			JsonNode value = REDIS_OBJECT_MAPPER.readTree(Files.readAllBytes(sample));
			byte[] raw = REDIS_OBJECT_MAPPER.writeValueAsBytes(value);
			byte[] stored = serializer.serialize(value);

			assertThat(serializer.deserialize(stored)).isEqualTo(value);

			totalRaw += raw.length;
			totalStored += stored.length;
			System.out.printf("%-40s %10d %10d %7.1f%%%n",
				sample.getFileName(), raw.length, stored.length, savedPercent(raw.length, stored.length));
		}

		System.out.printf("%-40s %10d %10d %7.1f%%%n",
			"TOTAL (" + samples.size() + " samples)", totalRaw, totalStored, savedPercent(totalRaw, totalStored));
	}

	private static List<Path> findSamples() throws IOException {
		if (!Files.isDirectory(SAMPLE_DIR)) {
			return List.of();
		}
		try (Stream<Path> files = Files.list(SAMPLE_DIR)) {
			return files.filter(path -> path.toString().endsWith(".json")).sorted().toList();
		}
	}

	private static double savedPercent(final long raw, final long stored) {
		return (1 - (double)stored / raw) * 100;
	}
}
