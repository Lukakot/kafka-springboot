package com.kafkastreams.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkastreams.domain.Order;
import com.kafkastreams.domain.OrderLineItem;
import com.kafkastreams.domain.OrderType;
import com.kafkastreams.service.OrderService;
import org.apache.kafka.streams.KeyValue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.kafkastreams.topology.OrdersTopology.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(topics = {ORDERS, STORES})
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD) //ApplicationContext associated with the test should be closed and removed after the test method (or class) has run. Prevents test fails when run all at once
public class OrdersTopologyIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OrderService orderService;


    @BeforeEach
    void setUp(){
        streamsBuilderFactoryBean.start();
    }

    @AfterEach
    void tearDown(){
        Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).close();
        streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
    }


    @Test
    void ordersCount(){
        //given
        publishOrders();

        //then
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCount(GENERAL_ORDERS).size(), equalTo(1));

        var generalOrderCount = orderService.getOrdersCount(GENERAL_ORDERS);
        assertEquals(1, generalOrderCount.get(0).orderCount());


    }

    @Test
    void ordersRevenue(){
        //given
        publishOrders();

        //then
        Awaitility.await().atMost(15, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCount(GENERAL_ORDERS).size(), equalTo(1));

        Awaitility.await().atMost(15, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCount(RESTAURANT_ORDERS).size(), equalTo(1));


        var generalOrdersRevenue = orderService.revenueByOrderType(GENERAL_ORDERS);
        assertEquals(new BigDecimal("27.00"), generalOrdersRevenue.get(0).totalRevenue().runningRevenue());

        var restaurantOrdersRevenue = orderService.revenueByOrderType(RESTAURANT_ORDERS);
        assertEquals(new BigDecimal("15.00"), restaurantOrdersRevenue.get(0).totalRevenue().runningRevenue());

    }


    static List<KeyValue<String, Order>> orders() {

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.now()
        );
        var keyValue1 = KeyValue.pair(order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair(order2.orderId().toString()
                , order2);


        return List.of(keyValue1, keyValue2);

    }

    private void publishOrders() {
        orders()
                .forEach(order -> {
                    String orderJSON;
                    try {
                        orderJSON = objectMapper.writeValueAsString(order.value);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    kafkaTemplate.send(ORDERS, order.key, orderJSON);
                });


    }

}
