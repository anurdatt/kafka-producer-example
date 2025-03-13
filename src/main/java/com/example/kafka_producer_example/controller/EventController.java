package com.example.kafka_producer_example.controller;

import com.example.kafka_producer_example.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publish(@PathVariable String message) {
        try {
            for (int i=1; i<=100000; i++) {
                publisher.sendMessage(message + i);
            }
            return ResponseEntity.ok("Message sent to the topic");
        } catch (Exception ex) {
            return ResponseEntity.status(500).body("Unknown Server error! - " + ex.getMessage());
        }
    }
}
