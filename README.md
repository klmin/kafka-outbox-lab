# kafka-outbox-lab

Kafka로 메시지를 **잃지 않고, 두 번 처리하지 않게** 만드는 방법을 케이스별로 직접 재현하고 검증하는 실험 저장소.

- 각 케이스는 "문제 재현 테스트 → 해결 → 같은 테스트 통과 → 결과 기록" 순서로 진행한다.
- 기본(내부 구조, 용어, Spring Boot 설정, 상황별 사용): [docs/kafka-basics.md](docs/kafka-basics.md)
- 패턴 정리(전달 보장, 아웃박스, 멱등, DLT): [docs/concepts.md](docs/concepts.md)
- 케이스 목록과 기록: [docs/cases](docs/cases/README.md)

## 스택

Java 25, Spring Boot 4.2, Spring Kafka, Apache Kafka 4.4(KRaft), PostgreSQL 18, Flyway, Testcontainers

## 실행

```bash
# 테스트 (Docker 필요. Testcontainers 가 Kafka, PostgreSQL 을 띄운다)
./gradlew test

# 로컬 실행 (compose.yaml 의 Kafka, PostgreSQL, Kafka UI 를 자동으로 띄운다)
./gradlew bootRun
```

- Kafka UI: http://localhost:8989 (토픽, 메시지, 컨슈머 그룹 lag)
- 포맷: `./gradlew spotlessApply`

## CI

- `ci.yml`: PR, main push 마다 `./gradlew build` (포맷 검사 + Testcontainers 통합 테스트)
- `codex-review-on-push.yml`: 열린 PR 에 커밋을 push 할 때마다 `@codex review` 댓글로 리뷰 요청 (secret `CODEX_TRIGGER_PAT` 필요)
