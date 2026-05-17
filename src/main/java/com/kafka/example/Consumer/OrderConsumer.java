package com.kafka.example.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.example.dto.OrderRepository;
import com.kafka.example.model.Order;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderConsumer {

    private final KafkaTemplate<String,String> kafkaTemplate;

    private final OrderRepository orderRepository;

    private final ObjectMapper mapper =
            new ObjectMapper();


   @KafkaListener(
           topics = "orders-topic",
           groupId = "order-group"
   )

    public void consume(String message) throws Exception {

        Order order = mapper.readValue(message,Order.class);

        //save to database
        orderRepository.save(order);

        System.out.println("Recieved:" +order);

        //Processed Orders

       String processedMessage =
               "PROCESSED --> " + message;

       kafkaTemplate.send(
               "processed-orders-topic",
               processedMessage
       );

       System.out.println("Message Sent to processed order topic");
    }
}