package com.kafkastreams.service;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class OrderStoreService {

    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    public OrderStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean){
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public ReadOnlyKeyValueStore<String, Long> ordersCountStore(String storeName) {

        return Objects
                .requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.keyValueStore())
                );
    }
}
