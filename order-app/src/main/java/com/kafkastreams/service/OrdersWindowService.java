package com.kafkastreams.service;

import com.kafkastreams.domain.OrderType;
import com.kafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.kafkastreams.domain.TotalRevenue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.kafkastreams.service.OrderService.mapOrderType;
import static com.kafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrdersWindowService {

    private final OrderStoreService orderStoreService;

    @Autowired
    public OrdersWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {

        var countWindowsStore = getCountWidowStore(orderType);
        var orderTypeEnum = mapOrderType(orderType);

        var countWindowsIterator = countWindowsStore.all();

        if (countWindowsIterator.hasNext()){
            var iterationField = countWindowsIterator.next();
            log.info("Iterator key: {}, value: {}", iterationField.key.key(), iterationField.value);
        }
        return mapToOrderCountPerStoreByWindowsDTO(countWindowsIterator, orderTypeEnum);
    }

    private static List<OrdersCountPerStoreByWindowsDTO> mapToOrderCountPerStoreByWindowsDTO(KeyValueIterator<Windowed<String>, TotalRevenue> countWindowsIterator, OrderType orderTypeEnum) {
        var keyValueSpliterator = Spliterators.spliteratorUnknownSize(countWindowsIterator, 0);

        return StreamSupport.stream(keyValueSpliterator, false)
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(
                        keyValue.key.key(),
                        keyValue.value.runningOrderCount().longValue(),
                        orderTypeEnum,
                        LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                ))
                .collect(Collectors.toList());
    }

    private ReadOnlyWindowStore<String, TotalRevenue> getCountWidowStore(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsCountStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrderCountByWindow() {

        var generalOrdersCount = getOrdersCountWindowsByType(GENERAL_ORDERS);
        var restaurantOrdersCount = getOrdersCountWindowsByType(RESTAURANT_ORDERS);

        return Stream.of(restaurantOrdersCount, generalOrdersCount)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrderCountByWindow(LocalDateTime startTime, LocalDateTime endTime) {

        var startTimeInstant = startTime.toInstant(ZoneOffset.UTC);
        var endTimeInstant = endTime.toInstant(ZoneOffset.UTC);

        var generalOrdersCountByWindows = getCountWidowStore(GENERAL_ORDERS).fetchAll(startTimeInstant, endTimeInstant);
        var generalOrderCountByWindowsDTO = mapToOrderCountPerStoreByWindowsDTO(generalOrdersCountByWindows, OrderType.GENERAL);

        var restaurantOrdersCountByWindows = getCountWidowStore(RESTAURANT_ORDERS).fetchAll(startTimeInstant, endTimeInstant);
        var restaurantOrderCountByWindowsDTO = mapToOrderCountPerStoreByWindowsDTO(restaurantOrdersCountByWindows, OrderType.RESTAURANT);

        return Stream.of(generalOrderCountByWindowsDTO, restaurantOrderCountByWindowsDTO)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
