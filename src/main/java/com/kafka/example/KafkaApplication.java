package com.kafka.example;

import com.kafka.example.Consumer.OrderConsumer;
import com.kafka.example.Producer.OrderProducer;
import com.kafka.example.config.KafkaConfig;
import com.kafka.example.model.Order;

public class KafkaApplication {

    public static void main(String[] args) throws Exception {

        System.out.println("Kafka Application Started");

        OrderProducer producer =
                new OrderProducer(
                        KafkaConfig.createProducer()
                );

        producer.sendOrder(
                new Order(
                        101,
                        "iPhone",
                        80000
                )
        );

        OrderConsumer consumer =
                new OrderConsumer(
                        KafkaConfig.createConsumer()
                );

        consumer.consumeOrders();
    }
}
