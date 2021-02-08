package com.knoldus.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.knoldus.models.User;
import org.apache.kafka.common.serialization.Serializer;

public class UserSerializer implements Serializer<User> {
    @Override
    public byte[] serialize(String s, User user) {
        byte[] userToByte = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            userToByte = objectMapper.writeValueAsString(user).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userToByte;
    }
}