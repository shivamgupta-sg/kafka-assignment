package com.knoldus.consumers;

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
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }

    public static void consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.utils.UserDeserializer");
        properties.put("group.id", "user-group");

        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer<>(properties);
        List topics = new ArrayList();
        topics.add("user");
        kafkaConsumer.subscribe(topics);
        try{
            // Message1
            while (true){
                ConsumerRecords<String, User> records = kafkaConsumer.poll(Duration.ofMinutes(1));
                for (ConsumerRecord<String, User> record: records){
                    System.out.printf("Topic - %s, Partition - %d, Value: %s%n", record.topic(), record.partition(), record.value());
                    // method that would be writing this value to a file.
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaConsumer.close();
        }
    }
}

class ConsumerListener implements Runnable {
    @Override
    public void run() {
        UserConsumer.consumer();
    }
}
