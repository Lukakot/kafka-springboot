package com.kafkastreams.domain;

import java.time.LocalDateTime;

public record OrdersCountPerStoreByWindowsDTO(String locationId,
                                              Long orderCount,
                                              OrderType orderType,
                                              LocalDateTime startWindow,
                                              LocalDateTime endWindow) {
}
