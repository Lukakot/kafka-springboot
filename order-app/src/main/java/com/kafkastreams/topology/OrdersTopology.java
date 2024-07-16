package com.kafkastreams.topology;

import com.kafkastreams.domain.*;
import com.kafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Slf4j
public class OrdersTopology {

    public static final String ORDERS = "orders";

    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";
    public static final String STORES = "stores";



    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        orderTopology(streamsBuilder);

    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {

        Predicate<String, Order> generalPredicate = (string, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (string, order) -> order.orderType().equals(OrderType.RESTAURANT);

        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Store.class)));

        storesTable
                .toStream()
                .print(Printed.<String,Store>toSysOut().withLabel("stores"));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));

        orderStreams
                .split(Named.as("general-restaurant"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                            generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("generalStream"));

                            aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindow(generalOrderStream, GENERAL_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersByRevenueByTimeWindow(generalOrderStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);
                        })
                )
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));

                            aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindow(restaurantOrderStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storesTable);
                            aggregateOrdersByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                            aggregateOrdersByRevenueByTimeWindow(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);
                        })
                );

    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storesTable) {
        var orderCountPerStore = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .count(Named.as(storeName), Materialized.as(storeName));

        orderCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        var revenueWithStoreTable = orderCountPerStore
                .join(storesTable, valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName+"-store"));
    }

    private static void aggregateOrdersCountByTimeWindow(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storesTable) {

        var windowSize = Duration.ofSeconds(15);
        var timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);
        var orderCountPerStore = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .windowedBy(timeWindows)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        orderCountPerStore
                .toStream()
                .peek((key, value) -> {
                    log.info("Store Name: {}, key: {}, value: {}", storeName, key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));

//        ValueJoiner<Long,Store,TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;
//        var revenueAndStoreTable = orderCountPerStore
//                .join(storesTable, valueJoiner);
//
//        revenueAndStoreTable
//                .toStream()
//                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-store"));
    }


    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storesTable) {
        Initializer<TotalRevenue> totalRevenueInitializer =
                TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        var revenueTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String,TotalRevenue, KeyValueStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))
                );

        //KTable-KTable
        ValueJoiner<TotalRevenue,Store,TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var revenueAndStoreTable = revenueTable
                .join(storesTable, valueJoiner);

        revenueAndStoreTable
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));
    }


    private static void aggregateOrdersByRevenueByTimeWindow(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storesTable) {

        var windowSize = Duration.ofSeconds(5);
        var timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        var revenueTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .windowedBy(timeWindows)
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String,TotalRevenue, WindowStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))
                );

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info("Store Name: {}, key: {}, value: {}", storeName, key, value);
                })
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut().withLabel(storeName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var joinedParams = Joined.with(Serdes.String(), new JsonSerde<>(TotalRevenue.class), new JsonSerde<>(Store.class));

        revenueTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storesTable, valueJoiner, joinedParams)
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-store"));
    }
}
