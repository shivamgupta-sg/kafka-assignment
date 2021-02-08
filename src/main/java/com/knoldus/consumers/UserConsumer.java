package com.knoldus.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.knoldus.models.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UserConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.utils.UserDeserializer");
        properties.put("group.id", "user-group");

        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer<>(properties);
        List topics = new ArrayList();
        topics.add("user");
        kafkaConsumer.subscribe(topics);
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            while (true) {
                ConsumerRecords<String, User> consumerRecords = kafkaConsumer.poll(Duration.ofMinutes(1));

                for (ConsumerRecord<String, User> consumerRecord : consumerRecords) {
                    System.out.printf(
                            "Topic: %s, Partition: %d, Value: %s%n",
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            consumerRecord.value().toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}