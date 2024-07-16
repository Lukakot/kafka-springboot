package com.kafkastreams.service;

import com.kafkastreams.domain.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.kafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrderService {

    private final OrderStoreService orderStoreService;

    @Autowired
    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
        var ordersCountPerStore = getOrderStore(orderType);
        var orders = ordersCountPerStore.all();

        var keyValueSpliterator = Spliterators.spliteratorUnknownSize(orders, 0);

        return StreamSupport.stream(keyValueSpliterator, false)
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

    }


    private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
