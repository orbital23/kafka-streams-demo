package com.pauldev.kafkastreamsdemo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Slf4j
public class KafkaConsumer implements ApplicationRunner {
    private static final String TOPIC_NAME = "mytopic";

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumer.class, args);
    }

    @KafkaListener(topics = TOPIC_NAME, groupId = "test-group")
    public void topicConsumer(ConsumerRecord<?, ?> userRecord) {
        log.info("Received : {} ", userRecord);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        while (true) {
            TimeUnit.SECONDS.sleep(60);
        }
    }
}
