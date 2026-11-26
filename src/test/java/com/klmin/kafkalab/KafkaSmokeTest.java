package com.klmin.kafkalab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 템플릿 동작 확인용: 실제 Kafka 에 보내고 리스너가 받는지만 본다.
 * 케이스별 실험은 docs/cases 를 보고 별도 테스트로 만든다.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, KafkaSmokeTest.SmokeListener.class})
class KafkaSmokeTest {

    static final String TOPIC = "smoke";

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    SmokeListener listener;

    @Test
    void 보낸_메시지를_리스너가_받는다() throws Exception {
        kafkaTemplate.send(TOPIC, "order-1", "hello").get();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(listener.received)
                        .singleElement()
                        .satisfies(r -> {
                            assertThat(r.key()).isEqualTo("order-1");
                            assertThat(r.value()).isEqualTo("hello");
                        }));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class SmokeListener {

        final List<ConsumerRecord<String, String>> received = new CopyOnWriteArrayList<>();

        @Bean
        NewTopic smokeTopic() {
            return TopicBuilder.name(TOPIC).partitions(1).replicas(1).build();
        }

        @KafkaListener(topics = TOPIC, groupId = "smoke-test")
        void onMessage(ConsumerRecord<String, String> record) {
            received.add(record);
        }
    }
}
