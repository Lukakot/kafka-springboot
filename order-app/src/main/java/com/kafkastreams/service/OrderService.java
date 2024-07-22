package com.kafkastreams.service;

import com.kafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String locationId) {

        var ordersCountStore = getOrderStore(orderType);
        var orderCount = ordersCountStore.get(locationId);

        if(orderCount!= null){
            return new OrderCountPerStoreDTO(locationId, orderCount);
        }
        return null;
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> mapper = (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
                orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(), orderType
        );

        var generalOrders = getOrdersCount(GENERAL_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                .toList();

        var restaurantOrders = getOrdersCount(RESTAURANT_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
                .toList();

        return Stream.of(generalOrders, restaurantOrders)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrderRevenueDTO> revenueByOrderType(String orderType) {
        var revenueStoreByType = getRevenueStore(orderType);
        var revenueIterator = revenueStoreByType.all();

        var keyValueSpliterator = Spliterators.spliteratorUnknownSize(revenueIterator, 0);

        return StreamSupport.stream(keyValueSpliterator, false)
                .map(keyValue -> new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
                .collect(Collectors.toList());
    }

    public static OrderType mapOrderType(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.ordersRevenueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.ordersRevenueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
