package com.cockroachlabs.university.columnfamilies.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class BankTransaction {
    
    private UUID transactionId;
    private UUID accountId;
    private LocalDateTime timestamp;
    private BigDecimal amount;
    private String transactionType;
    private String description;
    private String customerNotes;
    private String internalNotes;
    private byte[] attachments;
    private String auditTrail;

}
