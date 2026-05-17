package com.kafka.example.dto;

import com.kafka.example.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order,Integer> {

}
