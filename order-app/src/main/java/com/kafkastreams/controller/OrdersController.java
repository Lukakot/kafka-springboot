package com.kafkastreams.controller;

import com.kafkastreams.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;



@RestController
@RequestMapping("/v1/orders")
public class OrdersController {


    private final OrderService orderService;

    @Autowired
    public OrdersController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> ordersCount(@PathVariable("order_type") String orderType,
                                      @RequestParam(value = "location_id", required = false) String locationId){

        if(StringUtils.hasLength(locationId)){
            return ResponseEntity.ok(orderService.getOrdersCountByLocationId(orderType, locationId));
        }
        return ResponseEntity.ok(orderService.getOrdersCount(orderType));
    }



}
