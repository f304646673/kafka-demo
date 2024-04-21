package com.example.kafkademo.kafka_operations;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class KafkaOperationTest {

    @Autowired
    private KafkaOperation kafkaOperation;

    @Test
    public void testSendMessage() {
        assertTrue(kafkaOperation.sendMessage("test-topic", "Hello World!"));
    }

    @Test
    public void testSendMessageWithKey() {
        kafkaOperation.sendMessage("test-topic", "key", "Hello World!");
    }

    @Test
    public void testSendMessageWithPartition() {
        kafkaOperation.sendMessage("test-topic", 0, "key", "Hello World!");
    }

    @Test
    public void testSendMessageWithTimestamp() {
        kafkaOperation.sendMessage("test-topic", 0, System.currentTimeMillis(), "key", "Hello World!");
    }

    @Test
    public void testConsumeMessage() {
        kafkaOperation.consumeMessage("Hello World!");
    }
    
}
