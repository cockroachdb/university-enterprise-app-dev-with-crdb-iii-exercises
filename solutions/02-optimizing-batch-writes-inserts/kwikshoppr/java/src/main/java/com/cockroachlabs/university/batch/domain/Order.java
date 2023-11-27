package com.cockroachlabs.university.batch.domain;

import java.util.UUID;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Order {

    private UUID id;
    private UUID cart_id;
    private String status;
}
