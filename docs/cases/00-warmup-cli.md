# 00. 준비 실습: CLI 로 보내고 받기

코드 없이 Kafka CLI 와 Kafka UI 만으로 [kafka-basics.md](../kafka-basics.md) §1~3 의 개념을 눈으로 확인한다.
케이스 01 전에 한 번 해 본다. 30분 정도 걸린다.

## 질문
Kafka 는 정말 "큐가 아니라 로그"인가? key, 파티션, offset, 컨슈머 그룹, lag 는 실제로 어떻게 움직이나?

## 가설
- 같은 key 는 같은 파티션에 들어가고, 파티션 안에서는 보낸 순서대로 읽힌다.
- 다른 그룹은 같은 메시지를 각자 처음부터 전부 읽는다. 읽어도 메시지는 지워지지 않는다.
- 같은 그룹의 컨슈머 둘은 파티션을 나눠 갖는다.
- 그룹이 멈춘 사이에 보낸 메시지만큼 lag 가 생긴다. 오프셋을 되돌리면 같은 메시지를 다시 읽는다.

## 재현 방법

### 0. 준비
저장소 루트에서 PowerShell 을 연다. 터미널이 세 개 필요하다(보내기 1, 받기 2).

```powershell
docker compose up -d kafka kafka-ui
```

CLI 를 짧게 부르는 함수를 만든다. 터미널마다 한 번씩 실행한다.

```powershell
function kt { docker compose exec kafka "/opt/kafka/bin/$($args[0])" --bootstrap-server localhost:9092 @($args[1..99]) }
```

- Kafka UI: http://localhost:8989
- Kafka 4.4 에서는 콘솔 프로듀서·컨슈머의 `--property` 가 deprecated 다. 프로듀서는 `--reader-property`, 컨슈머는 `--formatter-property` 를 쓴다.

### 1. 토픽 만들기

```powershell
kt kafka-topics.sh --create --topic warmup --partitions 3 --replication-factor 1
```

```powershell
kt kafka-topics.sh --describe --topic warmup
```

확인: 파티션 0, 1, 2 가 보이고 셋 다 `Leader: 1`, `Isr: 1` 이다. 브로커가 1대라서 리더도 복제본도 전부 브로커 1이다.

### 2. key 를 붙여 보내기 (터미널 A)

```powershell
kt kafka-console-producer.sh --topic warmup --reader-property parse.key=true --reader-property key.separator=:
```

`>` 프롬프트에 한 줄씩 입력한다. 다 입력하면 Ctrl+C 로 빠져나온다.

```
order-1:created
order-2:created
order-1:paid
order-3:created
order-1:shipped
order-2:paid
```

이어서 key 없이 보낸다.

```powershell
kt kafka-console-producer.sh --topic warmup
```

```
click-a
click-b
click-c
```

- PowerShell 5.1 에서 `"a:1","b:2" | kt ...` 처럼 파이프로 넣으면 첫 줄 앞에 BOM 이 붙는다. key 가 `order-1` 이 아니라 `﻿order-1` 이 되어 다른 파티션에 갈 수 있다. 직접 입력하자.

### 3. 처음부터 읽기 (터미널 B)

```powershell
kt kafka-console-consumer.sh --topic warmup --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
```

실제 출력 예(파티션 번호는 해시 결과라 다를 수 있다):

```
Partition:0  Offset:0  order-2  created
Partition:0  Offset:1  order-3  created
Partition:0  Offset:2  order-2  paid
Partition:1  Offset:0  order-1  created
Partition:1  Offset:1  order-1  paid
Partition:1  Offset:2  order-1  shipped
Partition:1  Offset:3  null     click-a
Partition:1  Offset:4  null     click-b
Partition:1  Offset:5  null     click-c
```

확인할 것:
- `order-1` 세 개는 같은 파티션에서 created → paid → shipped 순서다. **같은 key 는 같은 파티션, 파티션 안에서는 순서 보장.**
- 전체 출력은 보낸 순서가 아니다. **파티션 사이의 순서는 보장되지 않는다.**
- offset 은 파티션마다 0 부터 따로 센다.
- key 없는 `click-*` 은 세 개가 한 파티션에 몰렸다. key 가 없으면 메시지마다 돌아가며 넣는 게 아니라, 배치 단위로 한 파티션에 붙여 넣는다(sticky partitioner). 배치가 바뀌면 다른 파티션으로 간다.

Ctrl+C 로 멈추고 같은 명령을 한 번 더 실행한다. 같은 9개가 또 나온다. **읽어도 지워지지 않는다(로그).**
그룹을 안 주면 콘솔 컨슈머는 매번 새 그룹(`console-consumer-12345`)을 만들기 때문이다.

### 4. 한 그룹에 컨슈머 둘 (터미널 B, C)

두 터미널에서 같은 그룹 `g1` 으로 실행한다.

```powershell
kt kafka-console-consumer.sh --topic warmup --group g1 --from-beginning --formatter-property print.key=true --formatter-property print.partition=true
```

터미널 A 에서 다시 key 를 붙여 보내기를 열고 `a:1`, `b:2`, `c:3`, `d:4`, `e:5` 를 보낸다.

확인할 것:
- 메시지가 B, C 로 **나뉘어** 온다. 같은 메시지가 양쪽에 오지는 않는다.
- 한쪽은 파티션 하나, 다른 쪽은 파티션 둘을 맡는다. 터미널 A 에서 확인한다.

```powershell
kt kafka-consumer-groups.sh --describe --group g1 --members
```

```
CONSUMER-ID              ...  #PARTITIONS
console-consumer-bd89... ...  1
console-consumer-2801... ...  2
```

- C 를 Ctrl+C 로 멈추고 몇 개 더 보낸다. **리밸런싱**이 일어나 B 가 파티션 셋을 다 받는다.

### 5. 다른 그룹은 따로 전부 읽는다

B 는 그대로 두고 C 에서 그룹 `g2` 로 실행한다.

```powershell
kt kafka-console-consumer.sh --topic warmup --group g2 --from-beginning --formatter-property print.key=true --formatter-property print.partition=true
```

확인: g1 이 이미 읽은 메시지까지 g2 가 처음부터 전부 읽는다. 그룹마다 offset 을 따로 가진다.

- `--from-beginning` 은 **커밋된 offset 이 없는 새 그룹**일 때만 효과가 있다. 이미 커밋된 그룹은 커밋된 위치부터 이어서 읽는다.
- `--from-beginning` 없이 새 그룹으로 붙으면 지금부터 들어오는 메시지만 받는다(`auto.offset.reset=latest`).

### 6. lag 만들기

B, C 를 모두 멈춘다. 그룹 g1 에 살아 있는 컨슈머가 없다. 터미널 A 에서 메시지를 5개 더 보낸 뒤 그룹 상태를 본다.

```powershell
kt kafka-consumer-groups.sh --describe --group g1
```

출력 예(숫자는 앞에서 보낸 메시지와 파티션 배정에 따라 다르다):

```
GROUP  TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
g1     warmup  0          3               4               1
g1     warmup  1          6               7               1
g1     warmup  2          3               6               3
```

- `CURRENT-OFFSET`: g1 이 커밋한 위치(다음에 읽을 offset)
- `LOG-END-OFFSET`: 파티션 끝
- `LAG` = 둘의 차이 = 아직 안 읽은 메시지 수

Kafka UI → Consumers → g1 에서도 같은 lag 가 보인다.
B 에서 g1 을 다시 실행하면 밀린 5개만 받고 lag 가 0 이 된다.

### 7. 오프셋 되돌리기 = 재처리

g1 컨슈머를 모두 멈춘 상태에서 실행한다. 살아 있으면 거부된다.

```powershell
kt kafka-consumer-groups.sh --group g1 --topic warmup --reset-offsets --to-earliest --dry-run
```

```powershell
kt kafka-consumer-groups.sh --group g1 --topic warmup --reset-offsets --to-earliest --execute
```

g1 을 다시 실행하면 이미 처리한 메시지를 **처음부터 다시** 받는다.
실제 서비스라면 주문을 두 번 처리한 것이다. 그래서 멱등 처리가 필요하다 → 케이스 01, 06.

### 8. 정리

```powershell
kt kafka-topics.sh --delete --topic warmup
```

```powershell
kt kafka-consumer-groups.sh --delete --group g1 --group g2
```

```powershell
docker compose stop
```

## 결과
직접 해 보고 채운다.
- 같은 key 가 들어간 파티션:
- key 없는 메시지가 들어간 파티션:
- g1 컨슈머 둘의 파티션 분배:
- 6 단계의 lag:

## 한계, 남은 질문
- 콘솔 컨슈머는 자동 커밋(5초마다)이다. 처리 직후 커밋하는 Spring 설정(`ack-mode: record`)과 다르다. 커밋 시점에 따른 유실·중복은 케이스 01, 02 에서 확인한다.
- 브로커가 1대라 복제, ISR, 리더 선출은 볼 수 없다.

## 면접용 한 줄
Kafka 는 읽어도 지워지지 않는 로그이고, 컨슈머 그룹마다 offset 만 따로 기록한다. 그래서 여러 그룹이 같은 데이터를 독립적으로 읽고, offset 을 되돌려 재처리할 수 있다.
