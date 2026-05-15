package com.kafka.example.Producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.example.config.KafkaConfig;
import com.kafka.example.constants.KafkaConstants;
import com.kafka.example.model.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducer {

    private final KafkaProducer<String,String> producer;

    private final ObjectMapper mapper = new ObjectMapper();

    public OrderProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public void sendOrder(Order order) throws Exception{

        String json = mapper.writeValueAsString(order);

        ProducerRecord<String,String> record =
                new ProducerRecord<>(
                        KafkaConstants.ORDERS_TOPIC,
                        String.valueOf(order.getOrderID()),
                        json
                        );

        producer.send(record);

        System.out.println("Message Sent:" +json);
    }
}