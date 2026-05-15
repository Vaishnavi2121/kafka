package com.kafka.example.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.example.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

public class OrderConsumer {

    private final KafkaConsumer<String,String> consumer;

    private final ObjectMapper mapper =
            new ObjectMapper();


    public OrderConsumer(KafkaConsumer<String,String> consumer) {
      this.consumer = consumer;
    }

    public void consumeOrders() throws Exception{
        while (true){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String ,String> record: records){
                Order order = mapper.readValue(record.value(),Order.class);

                System.out.println(
                        "Received : " + order
                );
            }
        }
    }
}