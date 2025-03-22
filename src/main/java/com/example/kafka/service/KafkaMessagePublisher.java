package com.example.kafka.service;

import com.example.kafka.model.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka-topic-name}")
    String topicName;

    public void sendMessage(String message) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message = [" + message + "] " +
                        "with offset = [" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message = [" + message + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendEvent(Employee employee) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, employee);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent event = [" + employee + "] " +
                        "with offset = [" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send event = [" + employee + "] due to : " + ex.getMessage());
            }
        });
    }
}
