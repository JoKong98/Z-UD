# GZIP 압축률 실측 샘플

`GzipCompressionMeasurementTest`가 이 폴더의 `*.json` 파일을 읽어 원본 크기와 Redis 저장 크기를 비교합니다.

## 무엇을 넣나

Redis에 실제로 저장되는 값과 같은 JSON입니다. 저장 크기 대부분은 AI 응답 본문이 차지하므로, Kafka 응답 메시지 본문을 그대로 넣어도 됩니다.

- 리포트: `report-response` 토픽 메시지 (LLM 심사 결과)
- OCR: `ocr-response` 토픽 메시지 (서류 추출 결과)

서류 종류와 상품 수에 따라 크기가 크게 달라지니 여러 건을 넣을수록 숫자가 정확해집니다.

## 수집 예시

로컬에서 상담 1건을 끝까지 진행한 뒤 Kafka 컨테이너에서 메시지를 꺼냅니다.

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:80 --topic report-response \
  --from-beginning --max-messages 1 > report-1.json
```

`docker-compose.kafka.yml`의 `KAFKA_ADVERTISED_LISTENERS`는 운영 서버 주소(`j14a406.p.ssafy.io:80`)로 되어 있습니다. 로컬에서 띄웠다면 `PLAINTEXT://kafka:80`으로 바꿔야 위 명령이 동작합니다.

## 주의

테스트 계정 데이터라도 이름이나 주소 같은 개인정보가 들어 있을 수 있습니다. 이 폴더의 `*.json`은 `.gitignore`로 커밋에서 제외되어 있습니다.

## 실행

```bash
./gradlew test --tests "*GzipCompressionMeasurementTest" -i
```

`-i` 옵션을 붙여야 표가 출력됩니다.
