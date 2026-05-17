package com.kafka.example.controller;


import com.kafka.example.Producer.OrderProducer;
import com.kafka.example.model.Order;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer producer;


    @PostMapping
    public String placeOrder(@RequestBody Order order) throws Exception{
        producer.sendOrder(order);

        return "Order sent successfully";
    }
}
