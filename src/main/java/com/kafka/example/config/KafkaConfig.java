package com.kafka.example.config;

import com.kafka.example.constants.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.Properties;

public class KafkaConfig {

    public static KafkaProducer<String,String> createProducer(){

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String,String> createConsumer(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaConstants.BOOTSTRAP_SERVERS);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.ORDER_GROUP);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(KafkaConstants.ORDERS_TOPIC));

        return consumer;
    }

}
