package com.basickafka.kafkafirst.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Customer {

    UUID customerId;
    String name;
    String email;
    String phoneNumber;
}
