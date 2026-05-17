package com.kafka.example.Producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.example.model.Order;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String,String> kafkaTemplate;

    private final ObjectMapper mapper = new ObjectMapper();


    public void sendOrder(Order order) throws Exception{

        String json = mapper.writeValueAsString(order);

        kafkaTemplate.send(
                "orders-topic",
                json
        );

        System.out.println("Message Sent:" +json);
    }
}