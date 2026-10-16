package com.zud.backend.common.serializer;

import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.CRC32;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@DisplayName("GzipRedisSerializer 단위 테스트")
class GzipRedisSerializerTest {

	/**
	 * RedisConfig의 매퍼와 같은 설정. Jackson 3은 FAIL_ON_TRAILING_TOKENS가 기본으로 켜져 있어
	 * 복원 결과 뒤에 여분 바이트가 붙으면 역직렬화가 실패한다.
	 */
	private static final ObjectMapper REDIS_OBJECT_MAPPER = JsonMapper.builder()
		.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
		.build();

	private static final int COMPRESS_THRESHOLD = 2 * 1024;
	private static final int GZIP_TRAILER_SIZE = 8;
	private static final byte GZIP_MAGIC_FIRST = (byte)0x1f;
	private static final byte GZIP_MAGIC_SECOND = (byte)0x8b;

	/**
	 * {"v":"..."} 형태라 JSON 길이는 값 길이 + 8바이트다.
	 */
	private static final int JSON_OVERHEAD = 8;

	private final GzipRedisSerializer<Map<String, String>> serializer =
		new GzipRedisSerializer<>(REDIS_OBJECT_MAPPER, new TypeReference<>() {
		});

	@Nested
	@DisplayName("압축 기준")
	class Threshold {

		@Test
		@DisplayName("JSON이_정확히_2KB면_압축하지_않음")
		void JSON이_정확히_2KB면_압축하지_않음() {
			// given
			Map<String, String> value = valueOfJsonSize(COMPRESS_THRESHOLD);

			// when
			byte[] stored = serializer.serialize(value);

			// then
			assertThat(stored).hasSize(COMPRESS_THRESHOLD);
			assertThat(isGzip(stored)).isFalse();
			assertThat(serializer.deserialize(stored)).isEqualTo(value);
		}

		@Test
		@DisplayName("JSON이_2KB를_1바이트라도_넘으면_압축함")
		void JSON이_2KB를_1바이트라도_넘으면_압축함() {
			// given
			Map<String, String> value = valueOfJsonSize(COMPRESS_THRESHOLD + 1);

			// when
			byte[] stored = serializer.serialize(value);

			// then
			assertThat(isGzip(stored)).isTrue();
		}
	}

	@Nested
	@DisplayName("압축 완결성")
	class Completeness {

		@Test
		@DisplayName("압축_결과는_원본의_CRC32와_길이가_담긴_트레일러로_끝남")
		void 압축_결과는_원본의_CRC32와_길이가_담긴_트레일러로_끝남() {
			// given
			Map<String, String> value = valueOfJsonSize(10_000);
			byte[] original = REDIS_OBJECT_MAPPER.writeValueAsBytes(value);

			// when
			byte[] stored = serializer.serialize(value);

			// then
			// (Why) finish() 없이 바이트를 꺼내면 마지막 deflate 블록과 이 8바이트 트레일러가 빠진다.
			ByteBuffer trailer = ByteBuffer.wrap(stored, stored.length - GZIP_TRAILER_SIZE, GZIP_TRAILER_SIZE)
				.order(ByteOrder.LITTLE_ENDIAN);
			CRC32 crc32 = new CRC32();
			crc32.update(original);

			assertThat(Integer.toUnsignedLong(trailer.getInt())).isEqualTo(crc32.getValue());
			assertThat(trailer.getInt()).isEqualTo(original.length);
		}

		@Test
		@DisplayName("압축한_값은_원래_값으로_복원됨")
		void 압축한_값은_원래_값으로_복원됨() {
			// given
			Map<String, String> value = Map.of(
				"reason", "소득 대비 상환 비율이 기준을 넘지 않아 승인 가능합니다. ".repeat(100),
				"status", "COMPLETED"
			);

			// when
			byte[] stored = serializer.serialize(value);

			// then
			assertThat(isGzip(stored)).isTrue();
			assertThat(serializer.deserialize(stored)).isEqualTo(value);
		}

		@Test
		@DisplayName("복원_버퍼보다_작은_데이터도_여분_바이트_없이_복원됨")
		void 복원_버퍼보다_작은_데이터도_여분_바이트_없이_복원됨() {
			// given
			// (Why) 3000바이트를 복원하면 내부 버퍼는 4096바이트로 커진다. 버퍼를 통째로 돌려주면
			// 뒤의 빈 바이트 1096개까지 넘어가고, FAIL_ON_TRAILING_TOKENS 때문에 역직렬화가 실패한다.
			Map<String, String> value = valueOfJsonSize(3_000);

			// when
			byte[] stored = serializer.serialize(value);

			// then
			assertThat(serializer.deserialize(stored)).isEqualTo(value);
		}
	}

	@Nested
	@DisplayName("null 처리")
	class NullHandling {

		@Test
		@DisplayName("null은_그대로_null로_다룸")
		void null은_그대로_null로_다룸() {
			assertThat(serializer.serialize(null)).isNull();
			assertThat(serializer.deserialize(null)).isNull();
		}
	}

	private static Map<String, String> valueOfJsonSize(final int jsonSize) {
		char[] chars = new char[jsonSize - JSON_OVERHEAD];
		Arrays.fill(chars, 'a');
		return Map.of("v", new String(chars));
	}

	private static boolean isGzip(final byte[] bytes) {
		return bytes.length > 2 && bytes[0] == GZIP_MAGIC_FIRST && bytes[1] == GZIP_MAGIC_SECOND;
	}
}
