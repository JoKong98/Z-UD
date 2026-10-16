package com.zud.backend.common.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.StreamUtils;

import com.zud.backend.common.error.ErrorCode;
import com.zud.backend.common.error.exception.BusinessException;

import lombok.RequiredArgsConstructor;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

@RequiredArgsConstructor
public class GzipRedisSerializer<T> implements RedisSerializer<T> {

	private static final byte[] GZIP_MAGIC_BYTES = new byte[] {
		(byte)(GZIPInputStream.GZIP_MAGIC & 0xFF),
		(byte)((GZIPInputStream.GZIP_MAGIC >> 8) & 0xFF)
	};
	private static final int MIN_COMPRESS_SIZE = 2 * 1024; // 2KB 기준
	private static final int BUFFER_SIZE = 2 * 1024; // 버퍼 크기
	private final ObjectMapper objectMapper;
	private final TypeReference<T> typeRef;

	@Override
	public byte[] serialize(T value) {
		if (value == null) {
			return null;
		}

		try {
			byte[] bytes = objectMapper.writeValueAsBytes(value);
			return bytes.length > MIN_COMPRESS_SIZE ? compress(bytes) : bytes;
		} catch (BusinessException ex) {
			throw ex;
		} catch (Exception _) {
			throw new BusinessException(ErrorCode.SERIALIZE_ERROR);
		}
	}

	@Override
	public T deserialize(byte[] bytes) {
		if (bytes == null) {
			return null;
		}

		try {
			byte[] data = isGzipCompressed(bytes) ? decompress(bytes) : bytes;
			return objectMapper.readValue(data, typeRef);
		} catch (BusinessException ex) {
			throw ex;
		} catch (Exception _) {
			throw new BusinessException(ErrorCode.DESERIALIZE_ERROR);
		}
	}

	private byte[] compress(byte[] original) {
		try (
			ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
			GZIPOutputStream gos = new GZIPOutputStream(bos, BUFFER_SIZE)
		) {
			gos.write(original);
			// (Why) finish()로 남은 deflate 블록과 트레일러(CRC32·ISIZE)를 기록해야 완결된 GZIP이 된다.
			gos.finish();
			return bos.toByteArray();
		} catch (Exception _) {
			throw new BusinessException(ErrorCode.GZIP_COMPRESS_ERROR);
		}
	}

	private byte[] decompress(byte[] encoded) {
		try (
			GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(encoded), BUFFER_SIZE);
			ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE)
		) {
			StreamUtils.copy(gis, out);
			// (Why) 내부 버퍼를 그대로 돌려주면 유효 길이 뒤의 빈 바이트까지 넘어가 역직렬화가 실패한다.
			return out.toByteArray();
		} catch (Exception _) {
			throw new BusinessException(ErrorCode.GZIP_DECOMPRESS_ERROR);
		}
	}

	private boolean isGzipCompressed(byte[] bytes) {
		return bytes.length > 2 && bytes[0] == GZIP_MAGIC_BYTES[0] && bytes[1] == GZIP_MAGIC_BYTES[1];
	}
}