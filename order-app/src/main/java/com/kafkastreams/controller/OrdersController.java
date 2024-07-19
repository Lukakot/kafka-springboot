package com.kafkastreams.controller;

import com.kafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.kafkastreams.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;


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

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> revenueByOrderType(@PathVariable("order_type") String orderType) {
        return ResponseEntity.ok(orderService.revenueByOrderType(orderType));
    }

    @GetMapping("/count")
    public List<AllOrdersCountPerStoreDTO> allOrdersCount(){

        return orderService.getAllOrdersCount();
    }



}
