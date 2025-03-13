package com.example.kafka_producer_example.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessage(String message) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("kafka-topic2", message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message = [" + message + "] " +
                        "with offset = [" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message = [" + message + "] due to : " + ex.getMessage());
            }
        });
    }
}
