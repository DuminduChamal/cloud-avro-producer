package com.learning.kafka;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@OpenAPIDefinition(
        info = @Info(
                title = "Cloud Avro Producer API",
                version = "1.0",
                description = "Sends OrderEvent messages to Confluent Cloud, serialized as Avro and validated against Schema Registry."
        )
)
@SpringBootApplication
public class CloudAvroProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(CloudAvroProducerApplication.class, args);
    }
}