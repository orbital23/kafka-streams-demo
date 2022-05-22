package com.pauldev.kafkastreamsdemo.producer;

import com.pauldev.kafkastreamsdemo.pojo.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Slf4j
public class KafkaProducer implements ApplicationRunner {

    private static final String TOPIC_NAME = "mytopic";

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducer.class, args);
    }

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> users = Arrays.asList("david", "john", "raj", "peter");
        Random random = new Random();
        while (true) {
            User user = new User(users.get(random.nextInt(users.size())), random.nextInt(100));
            log.info("Sending user: {}", user);
            kafkaTemplate.send(TOPIC_NAME, user);
            TimeUnit.SECONDS.sleep(5);
        }
    }
}
