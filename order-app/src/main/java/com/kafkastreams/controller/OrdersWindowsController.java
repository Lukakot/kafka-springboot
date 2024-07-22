package com.kafkastreams.controller;

import com.kafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.kafkastreams.service.OrdersWindowService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/orders")
public class OrdersWindowsController {


    private final OrdersWindowService ordersWindowService;

    public OrdersWindowsController(OrdersWindowService ordersWindowService) {
        this.ordersWindowService = ordersWindowService;
    }

    @GetMapping("/windows/count/{order_type}")
    public List<OrdersCountPerStoreByWindowsDTO> ordersCount(
            @PathVariable("order_type") String orderType
    ){
        return ordersWindowService.getOrdersCountWindowsByType(orderType);
    }
}
