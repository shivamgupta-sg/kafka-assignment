package com.knoldus.producers;

import com.knoldus.models.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class UserProducer {
    public static void main(String[] args) {
        // For example 192.168.1.1:9092,192.168.1.2:9092
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.utils.UserSerializer");

        KafkaProducer<String, User> kafkaProducer = new KafkaProducer<>(properties);

        try {
            for (int counter = 1; counter <= 10; counter++) {
                User user = new User(counter, "Shivam", (int) (20 + (Math.random() * 40)), "BTech");

                kafkaProducer.send(
                        new ProducerRecord(
                                "user",
                                String.valueOf(user.getId()),
                                user));

                System.out.println(user);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}
