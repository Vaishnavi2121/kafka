package com.kafka.KafkaExample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(
                "bootstrap.servers",
                "localhost:9094"
        );

        props.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        props.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        "orders-topic",
                        "1",
                        "iPhone Order"
                );

        producer.send(record);

        System.out.println(
                "Message sent to orders-topic"
        );

        producer.close();
    }
}
