# Kafka 기본: 내부 구조, 용어, Spring Boot 설정

- 이 문서: Kafka 가 **어떻게 생겼고**, Spring Boot 에서 **어떻게 설정하고**, **언제 무엇을 쓰는지**.
- 패턴(아웃박스, 멱등, DLT 설계)은 [concepts.md](concepts.md), 직접 재현은 [cases](cases/README.md).
- 기준 버전: Apache Kafka 4.4(KRaft), Spring Boot 4.2, Spring Kafka 4.x. 기본값은 버전마다 바뀔 수 있으니 확인한 값은 케이스 기록에 남긴다.

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
| `acks` | `0`: 안 기다림, `1`: 리더만, `all`: ISR 전부 | 유실되면 안 되는 이벤트는 `all` (Kafka 3.0 부터 기본값) |
| `enable.idempotence` | 프로듀서 재전송으로 브로커에 중복 기록되는 것 방지 + 파티션 내 순서 유지 | `true` (3.0 부터 기본). **컨슈머 쪽 중복은 못 막는다** |
| `retries`, `delivery.timeout.ms` | 실패 시 재전송 횟수와 전체 제한 시간 | 기본값(무한 재시도, 2분) 유지가 보통 |
| `max.in.flight.requests.per.connection` | 응답 안 받고 보낼 수 있는 요청 수 | 멱등 프로듀서는 5 이하여야 순서 보장 |
| `linger.ms`, `batch.size` | 배치로 모으는 대기 시간과 크기 | 처리량↑ 지연↑ 트레이드오프 |
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
| `max.poll.records` | 한 번에 가져올 최대 건수(기본 500) | 처리가 느리면 줄인다 |
| `max.poll.interval.ms` | poll 사이 최대 시간(기본 5분) | 넘으면 리밸런싱 → 중복. 처리 시간 × max.poll.records 가 이보다 작아야 함 |
| `session.timeout.ms`, `heartbeat.interval.ms` | 컨슈머 생존 확인 | 너무 짧으면 GC 한 번에 리밸런싱 |
| `isolation.level` | `read_committed` 면 커밋된 트랜잭션 메시지만 읽음 | Kafka 트랜잭션 쓸 때 |
| `group.protocol` | `classic` / `consumer`(KIP-848 새 리밸런싱 프로토콜, 4.0 GA) | 새 프로토콜은 리밸런싱 중 전체 정지가 줄어듦 |

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
    var recoverer = new DeadLetterPublishingRecoverer(template);    // 기본: "<원래 토픽>.DLT", 같은 파티션 번호
    var backOff = new ExponentialBackOffWithMaxRetries(3);           // 1초, 2초, 4초 … 3번 재시도
    backOff.setInitialInterval(1_000);
    var handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(InvalidPayloadException.class); // 재시도해도 소용없는 오류는 바로 DLT
    return handler;
}
```
- Spring Boot 는 `CommonErrorHandler` 빈이 있으면 기본 리스너 컨테이너에 자동으로 연결한다.
- `DefaultErrorHandler` 기본값: 백오프 없이 9번 재시도(총 10번) 후 로그만 남기고 건너뜀 → **DLT 를 안 붙이면 조용히 유실된다.**
- 역직렬화 실패, 메시지 변환 실패 같은 예외는 기본으로 재시도하지 않는다.
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

## 8. 꼭 기억할 것 (면접 단골)

1. 순서는 **파티션 안에서만** 보장된다.
2. Kafka 기본 전달은 **at-least-once.** 처리 후 커밋해도 중복은 생긴다 → 멱등 처리.
3. 프로듀서 멱등성(`enable.idempotence`)은 **브로커 쪽 중복**만 막는다. 컨슈머 중복은 별개.
4. Kafka 트랜잭션은 **DB 를 묶지 못한다** → DB + 발행은 아웃박스.
5. `acks=all` + `min.insync.replicas=2` + RF 3 = 브로커 1대 장애에도 유실 없음.
6. `DefaultErrorHandler` 를 기본값으로 두면 재시도 후 **조용히 건너뛴다** → DLT 필수.
7. 처리가 `max.poll.interval.ms` 보다 오래 걸리면 리밸런싱 → 중복.
8. 컨슈머 수 > 파티션 수면 남는 컨슈머는 논다.
9. **lag** 은 반드시 모니터링한다.
