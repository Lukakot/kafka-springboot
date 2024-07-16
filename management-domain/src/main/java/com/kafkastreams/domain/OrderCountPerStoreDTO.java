package com.kafkastreams.domain;

public record OrderCountPerStoreDTO(String locationId,
                                    Long orderCount) {
}
