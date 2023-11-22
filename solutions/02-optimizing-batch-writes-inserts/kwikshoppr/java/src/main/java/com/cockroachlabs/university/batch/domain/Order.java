package com.cockroachlabs.university.batch.domain;

import java.util.UUID;

import org.jdbi.v3.core.annotation.JdbiProperty;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Order {

    @JdbiProperty(bind=false)
    private UUID id;
    
    private UUID cart_id;
    private String status;
}
