package com.kafkastreams.controller;

import com.kafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.kafkastreams.service.OrdersWindowService;
import org.springframework.cglib.core.Local;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
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


    @GetMapping("/windows/count")
    public List<OrdersCountPerStoreByWindowsDTO> getOrderCountByWindow(
            @RequestParam(value = "from_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
            @RequestParam(value = "to_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime
            ){

        if(startTime != null && endTime != null){
            return ordersWindowService.getOrderCountByWindow(startTime, endTime);
        }
        return ordersWindowService.getOrderCountByWindow();
    }
}
