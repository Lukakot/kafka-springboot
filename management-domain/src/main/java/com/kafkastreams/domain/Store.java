package com.kafkastreams.domain;

public record Store(String locationId,
                    Address address,
                    String contactNum) {
}
