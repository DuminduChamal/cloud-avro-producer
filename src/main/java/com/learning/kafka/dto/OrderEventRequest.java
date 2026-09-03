package com.learning.kafka.dto;

import io.swagger.v3.oas.annotations.media.Schema;

public record OrderEventRequest(
        @Schema(description = "Unique order identifier", example = "order-cloud-1") String orderId,
        @Schema(description = "Order amount", example = "55.00") double amount,
        @Schema(description = "Customer identifier", example = "cust-cloud") String customerId
) {}