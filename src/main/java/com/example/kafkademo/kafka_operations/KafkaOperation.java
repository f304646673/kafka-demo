package com.example.kafkademo.kafka_operations;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

@Component
public class KafkaOperation {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    public boolean sendMessage(String topic, String message) {
        CompletableFuture<SendResult<String,String>> resCompletableFuture = kafkaTemplate.send(topic, message);
        resCompletableFuture.whenComplete(new BiConsumer<SendResult<String,String>, Throwable>() {
            @Override
            public void accept(SendResult<String,String> result, Throwable ex) {
                if (ex != null) {
                    ex.printStackTrace();
                } else {
                    System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
                }
            }
        });
        return true;
    }

    public void sendMessage(String topic, String key, String message) {
        kafkaTemplate.send(topic, key, message);
    }

    public void sendMessage(String topic, Integer partition, String key, String message) {
        kafkaTemplate.send(topic, partition, key, message);
    }

    public void sendMessage(String topic, Integer partition, Long timestamp, String key, String message) {
        kafkaTemplate.send(topic, partition, timestamp, key, message);
    }

    @KafkaListener(topics = "test-topic", groupId = "group_id")
    public void consumeMessage(String message) {
        System.out.println("Consumed message: " + message);
    }

    @KafkaHandler(isDefault = true)
    public void consumeMessageDefault(String message) {
        System.out.println("Consumed message default: " + message);
    }
}
