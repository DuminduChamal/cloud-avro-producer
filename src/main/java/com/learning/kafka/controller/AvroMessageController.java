package com.learning.kafka.controller;

import com.learning.kafka.dto.OrderEventAvro;
import com.learning.kafka.dto.OrderEventRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AvroMessageController {

    private final KafkaTemplate<String, OrderEventAvro> kafkaTemplate;

    public AvroMessageController(KafkaTemplate<String, OrderEventAvro> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Operation(
            summary = "Send an order event to Confluent Cloud",
            description = "Builds an Avro OrderEventAvro record from the request, serializes it with KafkaAvroSerializer, and publishes it to cloud-avro-orders-topic."
    )
    @ApiResponse(responseCode = "200", description = "Message sent successfully, with the partition/offset it landed on")
    @PostMapping("/avro-messages")
    public ResponseEntity<String> sendAvroMessage(@RequestBody OrderEventRequest request) throws Exception {
        OrderEventAvro order = OrderEventAvro.newBuilder()
                .setOrderId(request.orderId())
                .setAmount(request.amount())
                .setTimestamp(System.currentTimeMillis())
                .setCustomerId(request.customerId())
                .build();

        SendResult<String, OrderEventAvro> result = kafkaTemplate
                .send("cloud-avro-orders-topic", order.getOrderId(), order)
                .get();

        return ResponseEntity.ok(String.format("Sent to Confluent Cloud -> partition=%d offset=%d",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));
    }
}