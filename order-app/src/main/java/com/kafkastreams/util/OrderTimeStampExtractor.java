package com.kafkastreams.util;

import com.kafkastreams.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.ZoneOffset;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var orderRecord = (Order) record.value();
        if(orderRecord!=null && orderRecord.orderedDateTime()!=null){
            var timeStamp = orderRecord.orderedDateTime();
            log.info("TimeStamp in extractor : {} ", timeStamp);
            var instant = timeStamp.toInstant(ZoneOffset.ofHours(-6)).toEpochMilli();
            log.info("instant in extractor : {} ", instant);
            return instant;
        }
        return partitionTime;
    }
}
