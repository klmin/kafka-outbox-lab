# Kafka 기본: 내부 구조, 용어, Spring Boot 설정

- 이 문서: Kafka 가 **어떻게 생겼고**, Spring Boot 에서 **어떻게 설정하고**, **언제 무엇을 쓰는지**.
- 패턴(아웃박스, 멱등, DLT 설계)은 [concepts.md](concepts.md), 직접 재현은 [cases](cases/README.md).
- 기준 버전: Apache Kafka 4.4(KRaft, kafka-clients 4.4.0), Spring Boot 4.2, Spring Kafka 4.2.0. 기본값은 버전마다 바뀔 수 있다.
- ✔ 표시는 이 저장소 의존성(jar)에서 직접 확인한 값(2026-11-27). 나머지는 케이스로 확인하면 ✔ 를 붙인다.

---

## 1. 전체 그림

```
Producer ──send(key, value)──▶  Topic "order-events"
                                 ├─ Partition 0 : [0][1][2][3][4] ...  ← 리더: Broker 1, 팔로워: Broker 2,3
                                 ├─ Partition 1 : [0][1][2] ...        ← 리더: Broker 2
                                 └─ Partition 2 : [0][1][2][3] ...     ← 리더: Broker 3
                                                   ▲ offset
Consumer Group "inventory"
  ├─ Consumer A ◀── Partition 0, 1
  └─ Consumer B ◀── Partition 2
Consumer Group "notification"  (다른 그룹은 같은 메시지를 따로 전부 읽는다)
  └─ Consumer C ◀── Partition 0, 1, 2
```

- Kafka 는 **큐가 아니라 로그**다. 읽어도 지워지지 않고, 보존 기간이 지나야 지워진다.
- 컨슈머는 "어디까지 읽었는지(offset)"만 기록한다. 그래서 여러 그룹이 같은 데이터를 각자 읽고, 오프셋을 되돌려 다시 읽을 수도 있다.

---

## 2. 핵심 용어

### 저장 구조
| 용어 | 뜻 | 중요한 점 |
|---|---|---|
| **Broker** | Kafka 서버 한 대 | 보통 3대 이상으로 클러스터 구성 |
| **Topic** | 메시지를 담는 논리적 이름(테이블 비슷) | 이벤트 종류별로 나눈다 |
| **Partition** | 토픽을 나눈 순서 있는 로그 | **병렬 처리 단위이자 순서 보장 단위.** 순서는 파티션 안에서만 보장 |
| **Offset** | 파티션 안 메시지 위치(0, 1, 2 …) | 파티션마다 따로 증가 |
| **Record(Message)** | key, value, headers, timestamp | key 로 파티션이 정해진다 |
| **Key** | 파티션 결정 기준 | 같은 key → 같은 파티션 → 순서 보장. 없으면 여러 파티션으로 흩어짐 |
| **Header** | 부가 정보(메시지 ID, trace ID 등) | 멱등 키, 이벤트 타입을 여기 싣는다 |
| **Segment** | 파티션을 디스크에 나눠 저장한 파일 | 보존 정책은 세그먼트 단위로 지운다 |

### 복제와 내구성
| 용어 | 뜻 | 중요한 점 |
|---|---|---|
| **Replica** | 파티션 복사본 | `replication.factor=3` 이면 3벌 |
| **Leader** | 읽기·쓰기를 받는 레플리카 | 파티션마다 하나 |
| **Follower** | 리더를 따라 복제하는 레플리카 | 리더가 죽으면 ISR 중 하나가 리더 |
| **ISR (In-Sync Replicas)** | 리더와 충분히 따라잡은 레플리카 집합 | `acks=all` 은 ISR 전부에 기록돼야 성공 |
| **min.insync.replicas** | 쓰기를 받기 위한 최소 ISR 수 | RF=3, min.isr=2, acks=all → 1대가 죽어도 유실 없이 쓰기 가능. 2대 죽으면 쓰기 거부 |
| **High Watermark** | 모든 ISR 에 복제된 마지막 위치 | 컨슈머는 여기까지만 읽는다 |
| **Unclean leader election** | ISR 밖 레플리카를 리더로 뽑기 | 켜면 가용성↑ 대신 유실 가능. 기본 꺼짐 |

### 클러스터 관리
| 용어 | 뜻 |
|---|---|
| **KRaft** | ZooKeeper 없이 Kafka 자체 Raft 합의로 메타데이터 관리. **Kafka 4.0 부터 ZooKeeper 제거, KRaft 만 지원** |
| **Controller** | 리더 선출, 토픽·파티션 메타데이터 관리 담당 노드(쿼럼) |
| `__consumer_offsets` | 컨슈머 그룹의 커밋된 오프셋을 저장하는 내부 토픽 |
| `__transaction_state` | Kafka 트랜잭션 상태를 저장하는 내부 토픽 |

### 소비
| 용어 | 뜻 | 중요한 점 |
|---|---|---|
| **Consumer Group** | 같은 `group.id` 를 쓰는 컨슈머 묶음 | 그룹 안에서 파티션 하나는 컨슈머 하나만 읽는다 |
| **Group Coordinator** | 그룹 멤버와 오프셋을 관리하는 브로커 | |
| **Rebalance** | 파티션을 컨슈머에게 다시 나눠 주는 과정 | 컨슈머 추가·종료·poll 지연 때 발생. **커밋 안 된 메시지는 중복 처리됨** |
| **Commit** | "여기까지 처리했다"를 `__consumer_offsets` 에 기록 | 처리 전 커밋 = 유실 위험, 처리 후 커밋 = 중복 위험 |
| **Lag** | 최신 offset − 커밋된 offset | 컨슈머가 밀리고 있다는 신호. 반드시 모니터링 |
| **Poll** | 컨슈머가 브로커에서 메시지를 가져오는 호출 | `max.poll.interval.ms` 안에 다음 poll 을 못 하면 그룹에서 쫓겨남 |

### 보관
| 용어 | 뜻 |
|---|---|
| **retention.ms** | 메시지 보존 기간(기본 7일). 처리 여부와 무관하게 시간이 지나면 삭제 |
| **cleanup.policy=delete** | 기간·크기 기준 삭제(기본) |
| **cleanup.policy=compact** | key 별 마지막 값만 남김. "현재 상태" 토픽(설정, 사용자 프로필 등)에 사용 |

---

## 3. 파티션 수와 컨슈머 수

- **컨슈머 수 ≤ 파티션 수**까지만 병렬이 늘어난다. 파티션 3개에 컨슈머 5개면 2개는 논다.
- 파티션은 **늘릴 수는 있어도 줄일 수 없다.** 늘리면 key → 파티션 매핑이 바뀌어 같은 key 의 순서가 잠깐 깨질 수 있다. 처음에 예상 처리량 기준으로 여유 있게 잡는다.
- 처리량이 부족하면: 파티션 수 늘리기, 컨슈머 처리 시간 줄이기, 배치 처리.

---

## 4. 프로듀서

### 보내는 흐름
`send()` → 직렬화 → 파티셔너(key 해시로 파티션 결정) → 배치에 모음 → 브로커 전송 → acks 받으면 성공 콜백

### 중요한 설정
| 설정 | 의미 | 권장 / 언제 |
|---|---|---|
| `acks` | `0`: 안 기다림, `1`: 리더만, `all`: ISR 전부 | 유실되면 안 되는 이벤트는 `all` (✔ 기본값, Kafka 3.0 부터) |
| `enable.idempotence` | 프로듀서 재전송으로 브로커에 중복 기록되는 것 방지 + 파티션 내 순서 유지 | `true` (✔ 기본, 3.0 부터). **컨슈머 쪽 중복은 못 막는다** |
| `retries`, `delivery.timeout.ms` | 실패 시 재전송 횟수와 전체 제한 시간 | 기본값(무한 재시도, 2분) 유지가 보통 |
| `max.in.flight.requests.per.connection` | 응답 안 받고 보낼 수 있는 요청 수 | 멱등 프로듀서는 5 이하여야 순서 보장 |
| `linger.ms`, `batch.size` | 배치로 모으는 대기 시간과 크기(✔ 기본 5ms, 16KB. linger.ms 기본은 Kafka 4.0 에서 0 → 5ms) | 처리량↑ 지연↑ 트레이드오프 |
| `compression.type` | `lz4`, `zstd` 등 | 큰 메시지, 높은 처리량일 때 |
| `transactional.id` | Kafka 트랜잭션 사용 | read-process-write exactly-once 가 필요할 때만 |

### 동기 vs 비동기 전송
- `kafkaTemplate.send(...).get()`: 결과를 기다림. 느리지만 실패를 바로 앎. 아웃박스 relay 처럼 "발행 확인 후 표시"할 때.
- `send(...).whenComplete(...)`: 콜백. 빠름. 실패 처리를 콜백에서 꼭 한다(무시하면 조용히 유실).

---

## 5. 컨슈머

### 중요한 설정
| 설정 | 의미 | 주의 |
|---|---|---|
| `group.id` | 컨슈머 그룹 | 같은 그룹 = 나눠 읽기, 다른 그룹 = 각자 전부 읽기 |
| `auto.offset.reset` | 커밋된 오프셋이 없을 때 시작 위치 | `earliest`(처음부터) / `latest`(지금부터). 신규 그룹이 과거 메시지를 놓치지 않으려면 `earliest` |
| `enable.auto.commit` | 주기적으로 자동 커밋 | Spring Kafka 는 기본으로 끄고 컨테이너가 커밋한다 |
| `max.poll.records` | 한 번에 가져올 최대 건수(✔ 기본 500) | 처리가 느리면 줄인다 |
| `max.poll.interval.ms` | poll 사이 최대 시간(기본 5분) | 넘으면 리밸런싱 → 중복. 처리 시간 × max.poll.records 가 이보다 작아야 함 |
| `session.timeout.ms`, `heartbeat.interval.ms` | 컨슈머 생존 확인(✔ 기본 45초, 3초) | 너무 짧으면 GC 한 번에 리밸런싱 |
| `isolation.level` | `read_committed` 면 커밋된 트랜잭션 메시지만 읽음 | Kafka 트랜잭션 쓸 때 |
| `group.protocol` | `classic`(✔ 기본) / `consumer`(KIP-848 새 리밸런싱 프로토콜, 4.0 GA) | 새 프로토콜은 리밸런싱 중 전체 정지가 줄어듦 |

---

## 6. Spring Boot 설정

### 6-1. application.yml (이 저장소 기준)
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092      # 로컬은 docker compose, 테스트는 @ServiceConnection 이 자동 설정
    producer:
      acks: all
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      properties:
        enable.idempotence: true
    consumer:
      group-id: kafka-outbox-lab
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 100
    listener:
      ack-mode: record       # 아래 표 참고
      concurrency: 3         # 리스너 스레드 수(= 파티션 수 이하로)
```
- `spring.kafka.producer.properties.*`, `consumer.properties.*` 로 전용 키가 없는 Kafka 설정을 그대로 넘긴다.

### 6-2. 보내기: KafkaTemplate
```java
kafkaTemplate.send("order-events", orderId, payload);              // 비동기
kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);                // 동기 (확인 필요할 때)

ProducerRecord<String, String> record = new ProducerRecord<>("order-events", orderId, payload);
record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8)); // 멱등 키
```

### 6-3. 받기: @KafkaListener
```java
@KafkaListener(topics = "order-events", groupId = "inventory")
void on(ConsumerRecord<String, String> record) { ... }      // 정상 반환 = 성공, 예외 = 에러 핸들러로

@KafkaListener(topics = "order-events")
void on(@Payload String body, @Header("messageId") String messageId) { ... }

@KafkaListener(topics = "order-events", batch = "true")    // 배치로 받기
void on(List<ConsumerRecord<String, String>> records) { ... }
```

### 6-4. 오프셋 커밋: AckMode
| AckMode | 커밋 시점 | 언제 |
|---|---|---|
| `BATCH` (기본) | poll 로 받은 묶음을 다 처리한 뒤 | 처리량 우선. 중간 실패 시 묶음 일부가 다시 옴 |
| `RECORD` | 레코드 하나 처리할 때마다 | 중복 범위를 최소화(이 저장소 기본) |
| `MANUAL` | `ack.acknowledge()` 호출 후, 다음 poll 전에 모아서 | 커밋 시점을 코드로 정하고 싶을 때 |
| `MANUAL_IMMEDIATE` | `ack.acknowledge()` 호출 즉시 | 위와 같지만 바로 커밋 |
| `TIME`, `COUNT`, `COUNT_TIME` | 시간·건수 기준 | 잘 안 씀 |

```java
@KafkaListener(topics = "order-events")
void on(ConsumerRecord<String, String> record, Acknowledgment ack) {
    service.handle(record);   // DB 트랜잭션 커밋까지 끝난 뒤
    ack.acknowledge();        // 그다음 오프셋 커밋 (ack-mode: manual)
}
```
- 어떤 모드든 "DB 커밋 후 오프셋 커밋 전" 틈은 남는다 → **멱등 처리가 필수.**

### 6-5. 에러 처리: DefaultErrorHandler + DLT
```java
@Bean
DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    var recoverer = new DeadLetterPublishingRecoverer(template);    // 기본: "<원래 토픽>-dlt"(✔ Spring Kafka 4.2. 예전 버전은 ".DLT"), 같은 파티션 번호
    var backOff = new ExponentialBackOffWithMaxRetries(3);           // 1초, 2초, 4초 … 3번 재시도
    backOff.setInitialInterval(1_000);
    var handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(InvalidPayloadException.class); // 재시도해도 소용없는 오류는 바로 DLT
    return handler;
}
```
- Spring Boot 는 `CommonErrorHandler` 빈이 있으면 기본 리스너 컨테이너에 자동으로 연결한다.
- `DefaultErrorHandler` 기본값(✔ `FixedBackOff(0, 9)`): 백오프 없이 9번 재시도(총 10번) 후 로그만 남기고 건너뜀 → **DLT 를 안 붙이면 조용히 유실된다.**
- ✔ 기본으로 재시도하지 않는 예외: `DeserializationException`, `MessageConversionException`, `ConversionException`, `MethodArgumentResolutionException`, `NoSuchMethodException`, `ClassCastException`.
- DLT 토픽은 원래 토픽 이상의 파티션 수가 필요하다(같은 파티션 번호로 보내기 때문).
- 이 방식은 **블로킹 재시도**: 재시도하는 동안 그 파티션 뒤 메시지가 밀린다.

### 6-6. 논블로킹 재시도: @RetryableTopic
```java
@RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, multiplier = 2))
@KafkaListener(topics = "order-events")
void on(String body) { ... }

@DltHandler
void dlt(String body, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) { ... }
```
- 실패한 메시지를 `order-events-retry-…` 토픽으로 옮기고 원래 파티션은 계속 진행 → 뒤 메시지가 막히지 않음.
- 대신 **순서가 깨진다.** 순서가 중요하면 블로킹(6-5), 처리량·지연이 중요하면 논블로킹.

### 6-7. 역직렬화 실패(poison pill) 막기: ErrorHandlingDeserializer
```yaml
spring:
  kafka:
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
```
- 없으면 깨진 메시지 하나에서 컨슈머가 계속 같은 위치를 읽다 실패하며 파티션이 멈춘다. 있으면 예외로 바꿔 에러 핸들러 → DLT 로 보낸다.

### 6-8. JSON
- Spring Kafka 4 는 Jackson 3 기반 `JacksonJsonSerializer` / `JacksonJsonDeserializer` 를 쓴다(예전 `JsonSerializer` 는 deprecated).
- 실무에서는 타입 헤더에 의존하기보다 문자열(JSON)로 보내고 받는 쪽에서 명시적으로 변환하거나, 스키마 레지스트리(Avro, Protobuf)를 쓴다.

### 6-9. 토픽 만들기
```java
@Bean
NewTopic orderEvents() {
    return TopicBuilder.name("order-events").partitions(3).replicas(1).build(); // 운영은 replicas 3
}
```
- 운영에서는 브로커 자동 생성(`auto.create.topics.enable`)을 끄고 명시적으로 만든다. 오타 난 토픽이 조용히 생기는 걸 막기 위해.

### 6-10. Kafka 트랜잭션
```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: tx-     # 설정하면 KafkaTransactionManager 가 생긴다
    consumer:
      isolation-level: read_committed
```
- 묶을 수 있는 것: **Kafka 에서 읽기 → 처리 → Kafka 에 쓰기 + 오프셋 커밋**(exactly-once).
- 묶을 수 없는 것: **DB 쓰기.** DB 와 Kafka 를 같이 묶어야 하면 아웃박스.

### 6-11. 테스트
- `@ServiceConnection` + Testcontainers `KafkaContainer`: 실제 Kafka 로 통합 테스트(이 저장소 `TestcontainersConfiguration`).
- `@EmbeddedKafka`(spring-kafka-test): 가볍지만 실제 브로커와 다를 수 있다.
- 비동기라 결과는 `Awaitility` 로 기다린다(`Thread.sleep` 금지).

---

## 7. 상황별로 무엇을 쓰나

| 상황 | 쓸 것 | 이유 |
|---|---|---|
| 주문 저장과 이벤트 발행이 같이 성공·실패해야 함 | 아웃박스 | DB 와 Kafka 는 한 트랜잭션 불가 |
| 같은 메시지가 두 번 와도 결과가 같아야 함 | 멱등 컨슈머(`processed_message` 또는 업무 키) | at-least-once 는 중복을 보장하지 않는다 |
| 같은 주문 이벤트 순서가 중요 | key = 주문 ID, 블로킹 재시도 | 순서는 파티션 안에서만 |
| 일시 장애(DB, 외부 API) | 백오프 재시도 | 잠깐 기다리면 성공 |
| 잘못된 데이터, 깨진 메시지 | 재시도 없이 DLT + 알림 | 몇 번 해도 실패 |
| 실패가 나도 뒤 메시지는 계속 처리 | `@RetryableTopic` | 대신 순서 포기 |
| Kafka → 가공 → Kafka (스트림 처리) | Kafka 트랜잭션 | exactly-once |
| 로그, 클릭 이벤트 (조금 빠져도 됨) | 바로 발행, `acks=1` 도 가능 | 처리량 우선 |
| key 별 최신 상태만 필요 | compacted 토픽 | 오래된 값 자동 정리 |
| 처리량 부족 | 파티션 + `concurrency` 증가, 배치 리스너 | 병렬 단위가 파티션 |

---

## 8. Kafka vs RabbitMQ

| | Kafka | RabbitMQ |
|---|---|---|
| 모델 | **로그.** 읽어도 남고 보존 기간이 지나야 삭제 | **큐.** 컨슈머가 ack 하면 삭제 |
| 가져오는 방식 | pull (컨슈머가 가져감) | push (브로커가 밀어줌, prefetch 로 조절) |
| 다시 읽기 | 오프셋을 되돌려 재처리 가능 | 한 번 ack 하면 끝 |
| 여러 소비자 | 그룹마다 같은 데이터를 각자 전부 읽음 | 큐를 나눠야 함(fanout exchange) |
| 순서 | 파티션 안에서 보장 | 큐 하나, 컨슈머 하나일 때 보장 |
| 라우팅 | key → 파티션 정도 | exchange(direct, topic, headers)로 유연한 라우팅 |
| 메시지 단위 기능 | 약함(지연 메시지, 우선순위 없음) | 우선순위, TTL, 지연(플러그인), 메시지별 ack |
| 처리량 | 매우 높음(초당 수십만~수백만) | 상대적으로 낮음 |
| 어울리는 곳 | 이벤트 스트리밍, 여러 서비스가 같은 이벤트를 구독, 재처리, 로그·지표 수집 | 작업 큐, 복잡한 라우팅, 요청-응답, 메시지별 처리 보장 |

- 답변 요령: "어느 게 낫다"가 아니라 **"이벤트를 여러 곳에서 다시 읽어야 하면 Kafka, 작업을 나눠 처리하고 라우팅이 복잡하면 RabbitMQ"**.
- RabbitMQ 도 Streams 기능으로 로그형 소비를 지원하지만, 기본 사용은 큐다.

---

## 9. Kafka 는 왜 빠른가

1. **순차 쓰기(append-only):** 파티션 끝에만 붙인다. 디스크도 순차 I/O 는 빠르다.
2. **OS 페이지 캐시:** 데이터를 JVM 힙이 아니라 OS 캐시에 둔다. 방금 쓴 데이터를 컨슈머가 바로 읽으면 디스크를 안 거친다. GC 부담도 적다.
3. **zero-copy(`sendfile`):** 페이지 캐시 → 소켓으로 바로 보내 사용자 공간 복사를 생략한다. (TLS 를 쓰면 암호화 때문에 적용 안 됨)
4. **배치와 압축:** 프로듀서가 묶어서 보내고, 브로커는 압축된 배치를 그대로 저장하고, 컨슈머가 푼다.
5. **파티션 병렬화:** 파티션마다 다른 브로커, 다른 컨슈머가 처리한다.
6. **브로커가 단순:** 메시지별 상태(누가 읽었는지)를 브로커가 관리하지 않고 컨슈머가 오프셋만 기록한다.

---

## 10. 파티션 할당 전략과 리밸런싱

| 전략 | 방식 | 특징 |
|---|---|---|
- ✔ 컨슈머 기본 `partition.assignment.strategy` 는 `[RangeAssignor, CooperativeStickyAssignor]` (classic 프로토콜 기준).

| `RangeAssignor` | 토픽별로 파티션을 연속 구간으로 나눔 | 여러 토픽을 같은 key 로 맞춰 받기 좋음. 불균형 생길 수 있음 |
| `RoundRobinAssignor` | 전체 파티션을 돌아가며 나눔 | 균등. 리밸런싱 때 많이 바뀜 |
| `StickyAssignor` | 균등 + 기존 할당 최대 유지 | 이동이 적음. 하지만 eager(아래) |
| `CooperativeStickyAssignor` | sticky + **점진적(cooperative) 리밸런싱** | 옮길 파티션만 내려놓고 나머지는 계속 처리 |

- **eager 리밸런싱:** 모든 컨슈머가 파티션을 전부 내려놓고 다시 받는다 → 그동안 그룹 전체 정지(stop-the-world).
- **cooperative 리밸런싱:** 옮겨야 하는 파티션만 넘긴다 → 정지 범위가 작다.
- **새 프로토콜(KIP-848, `group.protocol=consumer`, Kafka 4.0 GA):** 할당을 브로커(코디네이터)가 계산하고 점진적으로 적용한다. 클라이언트 assignor 대신 서버 쪽 assignor 를 쓴다.
- **정적 멤버십(`group.instance.id`):** 컨슈머가 재시작해도 `session.timeout.ms` 안에 돌아오면 리밸런싱하지 않는다. 롤링 배포 때 유용.
- 리밸런싱 직전에 처리한 만큼 커밋하려면 `ConsumerRebalanceListener.onPartitionsRevoked`(Spring Kafka 는 `ConsumerAwareRebalanceListener`)를 쓴다.

---

## 11. 메시지·이벤트 설계

### 이벤트 vs 커맨드
| | 이벤트 | 커맨드 |
|---|---|---|
| 의미 | **이미 일어난 일** | **해 달라는 요청** |
| 이름 | 과거형: `OrderCreated`, `PaymentCompleted` | 명령형: `ReserveStock`, `CancelPayment` |
| 받는 쪽 | 누가 듣든 상관없음(0~N) | 보통 특정 서비스 하나 |
| 거절 | 못 함(이미 일어남) | 실패, 거절 응답 가능 |

### 이벤트에 무엇을 담나
- **알림형(notification):** ID 만 보냄 → 받는 쪽이 API 로 다시 조회. 작지만 조회 부하와 결합이 생김.
- **상태 전달형(event-carried state transfer):** 필요한 데이터를 다 담음 → 받는 쪽이 독립적. 대신 크고 스키마 관리가 필요.
- 공통 필드: `eventId`(멱등 키), `eventType`, `aggregateId`(파티션 키), `occurredAt`, `version`(스키마 버전)

### 크기
- 브로커 `message.max.bytes`, 프로듀서 `max.request.size` 기본 약 1MB(✔ 프로듀서 1,048,576 바이트).
- 큰 데이터(파일, 이미지)는 **S3 에 두고 경로만 보낸다**(claim check 패턴). 한도를 키우는 건 마지막 수단.

### 스키마 변경
- 이미 나간 메시지와 아직 안 바뀐 컨슈머가 공존한다 → **호환성**을 지켜야 한다.
  - 하위 호환(backward): 새 컨슈머가 옛 메시지를 읽을 수 있음
  - 상위 호환(forward): 옛 컨슈머가 새 메시지를 읽을 수 있음
- 안전한 변경: **선택 필드 추가.** 위험한 변경: 필드 삭제, 이름·타입 변경, 필수 필드 추가.
- 큰 변경은 새 이벤트 타입(`OrderCreatedV2`)이나 새 토픽으로.
- 스키마 레지스트리(Avro, Protobuf)를 쓰면 호환성 검사를 등록 시점에 강제할 수 있다.

---

## 12. 운영 CLI (실험할 때 바로 쓰는 것)

compose 의 Kafka 컨테이너 안에서 실행한다. 스크립트 경로는 `apache/kafka` 이미지 기준 `/opt/kafka/bin`.

```bash
kt() { docker compose exec kafka /opt/kafka/bin/"$@"; }   # 예: kt kafka-topics.sh ...
BS="--bootstrap-server localhost:9092"

# 토픽
kt kafka-topics.sh $BS --list
kt kafka-topics.sh $BS --create --topic order-events --partitions 3 --replication-factor 1
kt kafka-topics.sh $BS --describe --topic order-events          # 파티션별 리더, ISR

# 보내기 (key:value 형식)
kt kafka-console-producer.sh $BS --topic order-events \
  --property parse.key=true --property key.separator=:

# 받기 (처음부터, key·헤더·파티션·오프셋까지 출력)
kt kafka-console-consumer.sh $BS --topic order-events --from-beginning \
  --property print.key=true --property print.headers=true \
  --property print.partition=true --property print.offset=true

# 컨슈머 그룹 상태 (CURRENT-OFFSET, LOG-END-OFFSET, LAG)
kt kafka-consumer-groups.sh $BS --list
kt kafka-consumer-groups.sh $BS --describe --group kafka-outbox-lab

# 오프셋 되돌리기 (재처리). 그룹의 컨슈머가 모두 멈춘 상태여야 한다
kt kafka-consumer-groups.sh $BS --group kafka-outbox-lab --topic order-events \
  --reset-offsets --to-earliest --dry-run      # 결과 미리 보기
kt kafka-consumer-groups.sh $BS --group kafka-outbox-lab --topic order-events \
  --reset-offsets --to-earliest --execute      # 실제 적용
# 다른 기준: --shift-by -10, --to-offset 42, --to-datetime 2026-11-27T00:00:00.000
```
- Windows PowerShell 에서는 함수 대신 `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ...` 로 그대로 쓴다.
- 오프셋 되돌리기는 **재처리 = 중복 처리**다. 멱등 처리가 돼 있어야 안전하다.

---

## 13. Graceful shutdown (배포 때 메시지 처리)

- 앱이 종료 신호(SIGTERM)를 받으면 Spring 이 리스너 컨테이너를 멈춘다.
  - 기본: 지금 처리 중인 레코드(또는 배치)를 끝내고 커밋한 뒤 멈춤. `spring.kafka.listener.immediate-stop=true` 면 현재 레코드까지만.
  - 컨테이너가 멈출 때까지 기다리는 시간은 `ContainerProperties.shutdownTimeout`(✔ 기본 10초). 처리가 길면 늘린다.
- 커밋 못 하고 죽으면 → 리밸런싱 후 다른 컨슈머가 재처리(중복). 그래서 여기서도 멱등 처리가 안전망.
- k8s 에서는 `terminationGracePeriodSeconds` 를 종료 대기 시간보다 길게 둔다. 짧으면 처리 도중 SIGKILL.
- 롤링 배포 때 리밸런싱이 연달아 일어나는 것을 줄이려면: cooperative 리밸런싱(또는 새 프로토콜) + 정적 멤버십.
- 프로듀서도 종료 때 `flush` 로 버퍼에 남은 메시지를 보낸다(Spring 이 `KafkaTemplate`/프로듀서 팩토리 종료 시 처리). 비동기 전송 결과를 확인하지 않으면 종료 직전 메시지가 유실될 수 있다.

---

## 14. 브로커 설정과 리스너 (compose.yaml 해설)

`application.yml` 은 **앱(클라이언트)** 설정, `compose.yaml` 의 `KAFKA_*` 는 **Kafka 서버(브로커)** 설정이다.
환경변수 `KAFKA_XXX_YYY` 는 서버 설정 `xxx.yyy` 로 바뀐다 (`KAFKA_NODE_ID` → `node.id`).
테스트는 Testcontainers `KafkaContainer` 가 비슷한 설정을 알아서 만들므로 compose.yaml 을 쓰지 않는다.

### 브로커와 컨트롤러

| 역할 | 하는 일 | 비유 |
|---|---|---|
| **Broker** | 파티션 데이터를 디스크에 저장하고, 프로듀서의 쓰기·컨슈머의 읽기를 처리하고, 다른 브로커와 복제한다 | 창고 + 창구 |
| **Controller** | 클러스터 **메타데이터** 관리: 어떤 브로커가 살아 있는지, 토픽·파티션 목록, 파티션마다 리더와 ISR. 브로커가 죽으면 새 리더를 뽑는다 | 관리 사무소 |

- 컨트롤러 여러 대가 Raft 로 합의한다(쿼럼). 그중 한 대가 active, 나머지는 대기하며 따라 적는다. 메타데이터는 내부 로그 `__cluster_metadata` 에 쌓인다.
- 과반이 살아 있어야 동작: 3대면 1대, 5대면 2대 장애까지 견딘다. 그래서 홀수로 둔다.
- 브로커는 컨트롤러에서 메타데이터를 받아 "내가 어느 파티션의 리더인지" 안다. 클라이언트는 컨트롤러에 직접 붙지 않는다.
- `process.roles=broker,controller` 는 한 프로세스가 둘 다 맡는 **combined 모드**. 로컬·테스트용이고, 운영은 역할을 나눈다.

### 리스너: 왜 포트가 여러 개인가

이 저장소 설정:
```yaml
KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:9093,EXTERNAL://:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,EXTERNAL://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

한 줄씩 형식은 `이름://호스트:포트`. **이름은 임의로 짓는 라벨**이다(`INTERNAL`, `DOCKER`, `HOST` 등 아무거나).

| 설정 | 뜻 |
|---|---|
| `listeners` | 브로커가 **실제로 여는 포트**. 프로세스 하나가 포트 3개를 연다 |
| `advertised.listeners` | 클라이언트에게 **"앞으로 이 주소로 접속해라"** 고 알려 주는 주소 |
| `listener.security.protocol.map` | **리스너 이름 → 보안 방식** 매핑. 이름은 라벨일 뿐이라 보안 방식을 여기서 정한다 |
| `controller.listener.names` | 어느 리스너가 컨트롤러 통신용인지 |
| `inter.broker.listener.name` | 브로커끼리 복제할 때 쓰는 리스너 |
| `controller.quorum.voters` | 컨트롤러 투표 멤버 목록, `노드ID@호스트:포트` 를 쉼표로. 3대면 `1@c1:9093,2@c2:9093,3@c3:9093` |

포트 3개의 용도:

| 이름 | 포트 | 누가 쓰나 | 알려 주는 주소 |
|---|---|---|---|
| `EXTERNAL` | 9092 | 내 PC 의 Spring 앱, CLI | `localhost:9092` |
| `PLAINTEXT` | 29092 | 같은 docker 네트워크의 컨테이너(Kafka UI), 브로커끼리 | `kafka:29092` |
| `CONTROLLER` | 9093 | 컨트롤러 쿼럼 통신 전용. 클라이언트는 안 씀 | 알려 주지 않음 |

- 9092 는 Kafka 클라이언트 포트의 관례, 9093 은 컨트롤러 포트의 관례일 뿐 정해진 값은 아니다. 호스트에는 9092 만 열었다(`ports: "9092:9092"`).
- `1@kafka:9093` = "노드 1번 컨트롤러는 `kafka` 호스트의 9093 포트". 자기 `node.id` 가 1이니 자기 자신 하나로 쿼럼이 된다.

**advertised 가 필요한 이유 (접속 2단계)**

```
1) 앱 → bootstrap-servers(localhost:9092) 로 접속, 메타데이터 요청
2) 브로커 → "파티션 0 리더는 localhost:9092" (접속한 리스너의 advertised 주소로 답한다)
3) 앱 → 그 주소로 다시 접속해서 produce / fetch
```

- 프로듀서, 컨슈머, AdminClient 모두 이 과정을 거친다. bootstrap 주소는 **처음 한 번 찾아가는 용도**일 뿐이다.
- 컨테이너 안의 Kafka UI 에게 `localhost` 는 자기 자신이다. 그래서 내부용 리스너(`kafka:29092`)를 따로 둔다.
- 흔한 장애: advertised 를 잘못 쓰면 **처음 접속은 되는데 메시지 보낼 때 타임아웃**. 2단계의 주소로 갈 수 없어서다.

### 보안 방식 (security.protocol)

| 값 | 암호화(TLS) | 인증 |
|---|---|---|
| `PLAINTEXT` | 없음(평문) | 없음 |
| `SSL` | 있음 | 없음, 또는 클라이언트 인증서(mTLS) |
| `SASL_PLAINTEXT` | 없음 | 있음 (SCRAM, PLAIN, OAUTHBEARER, Kerberos 등) |
| `SASL_SSL` | 있음 | 있음 ← **운영에서 주로 씀** |

- 암호화는 HTTPS 처럼 **연결(TLS) 단계**에서 클라이언트 라이브러리와 브로커가 처리한다. 앱 코드가 메시지를 직접 암호화·복호화하는 게 아니다.
- 클라이언트는 같은 방식으로 설정만 맞춘다. 예: `spring.kafka.security.protocol=SASL_SSL`, `spring.kafka.ssl.trust-store-location`, `spring.kafka.properties.sasl.mechanism`, `sasl.jaas.config`.
- `PLAINTEXT` 는 **리스너 이름이면서 보안 방식 이름**이라 헷갈린다. `PLAINTEXT:PLAINTEXT` 는 "PLAINTEXT 라는 이름의 리스너는 평문". 리스너 이름을 `INTERNAL` 로 바꾸면 `INTERNAL:PLAINTEXT` 가 된다.
- 보통 리스너마다 보안을 다르게 둔다. 예: 내부망은 `SASL_PLAINTEXT`, 외부 공개는 `SASL_SSL`.

### 단일 노드라서 바꾼 값

| 설정 | Kafka 기본값 | 여기 값 | 이유 |
|---|---|---|---|
| `offsets.topic.replication.factor` | 3 | 1 | 브로커 1대라 복제본 3개를 못 만든다. 그대로 두면 컨슈머 그룹이 동작하지 않음 |
| `transaction.state.log.replication.factor` / `.min.isr` | 3 / 2 | 1 / 1 | 같은 이유. Kafka 트랜잭션(케이스 12)용 |
| `group.initial.rebalance.delay.ms` | 3000 | 0 | 첫 컨슈머 합류 때 3초 대기 없이 바로 시작(실험 속도) |
| `num.partitions` | 1 | 3 | 자동 생성 토픽을 파티션 3개로. 순서(08), 리밸런싱(09) 실험용 |

- `apache/kafka` 이미지는 환경변수를 안 주면 자체 기본 설정으로 뜬다. 하지만 `localhost` 로만 접속돼서 Kafka UI 컨테이너가 붙지 못한다. 환경변수를 하나라도 주면 이미지 기본 설정 대신 환경변수 기준으로 설정하므로 위 항목을 다 적는다.

### 운영과 다른 점

| 항목 | 이 저장소(로컬) | 운영 |
|---|---|---|
| 노드 | 1대, broker+controller 겸용 | 브로커 3대 이상 + 컨트롤러 3대(따로) |
| 복제 | RF 1 | RF 3, `min.insync.replicas=2` (그래야 `acks=all` 이 "2대 이상 기록" 을 보장) |
| 보안 | PLAINTEXT | SASL_SSL + ACL(토픽별 읽기·쓰기 권한) |
| 토픽 | 자동 생성 허용 | `auto.create.topics.enable=false`, 명시적으로 생성 |

---

## 15. 꼭 기억할 것 (면접 단골)

1. 순서는 **파티션 안에서만** 보장된다.
2. Kafka 기본 전달은 **at-least-once.** 처리 후 커밋해도 중복은 생긴다 → 멱등 처리.
3. 프로듀서 멱등성(`enable.idempotence`)은 **브로커 쪽 중복**만 막는다. 컨슈머 중복은 별개.
4. Kafka 트랜잭션은 **DB 를 묶지 못한다** → DB + 발행은 아웃박스.
5. `acks=all` + `min.insync.replicas=2` + RF 3 = 브로커 1대 장애에도 유실 없음.
6. `DefaultErrorHandler` 를 기본값으로 두면 재시도 후 **조용히 건너뛴다** → DLT 필수.
7. 처리가 `max.poll.interval.ms` 보다 오래 걸리면 리밸런싱 → 중복.
8. 컨슈머 수 > 파티션 수면 남는 컨슈머는 논다.
9. **lag** 은 반드시 모니터링한다.
10. 클라이언트는 bootstrap 후 **advertised.listeners 주소로 다시 접속**한다. 잘못 쓰면 연결은 되는데 전송이 타임아웃.
