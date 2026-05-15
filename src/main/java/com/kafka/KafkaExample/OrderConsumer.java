package com.kafka.KafkaExample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {

        // =========================
        // Consumer Config
        // =========================

        Properties consumerProps = new Properties();

        consumerProps.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9094"
        );

        consumerProps.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "order-group"
        );

        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        consumerProps.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumerProps);

        consumer.subscribe(
                Collections.singletonList("orders-topic")
        );

        // =========================
        // Producer Config
        // =========================

        Properties producerProps = new Properties();

        producerProps.put(
                "bootstrap.servers",
                "localhost:9094"
        );

        producerProps.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        producerProps.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProps);

        System.out.println("Consumer Started...");

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {

                System.out.println(
                        "Received : " + record.value()
                );

                // Process Message
                String processedMessage =
                        "PROCESSED --> "
                                + record.value();

                // Publish to another topic
                producer.send(
                        new ProducerRecord<>(
                                "processed-orders-topic",
                                processedMessage
                        )
                );

                System.out.println(
                        "Published to processed-orders-topic"
                );
            }
        }
    }
}