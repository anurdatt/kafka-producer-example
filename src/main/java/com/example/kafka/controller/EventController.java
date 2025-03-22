package com.example.kafka.controller;

import com.example.kafka.model.Employee;
import com.example.kafka.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    @PostMapping("/publish")
    public ResponseEntity<?> publishEvent(@RequestBody Employee employee) {
        publisher.sendEvent(employee);
        return ResponseEntity.ok("Event sent to the topic");
    }
}
