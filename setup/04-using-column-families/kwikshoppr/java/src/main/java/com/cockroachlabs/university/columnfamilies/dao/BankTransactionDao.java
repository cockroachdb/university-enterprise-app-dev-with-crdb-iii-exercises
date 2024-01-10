package com.cockroachlabs.university.columnfamilies.dao;

import java.util.UUID;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.cockroachlabs.university.columnfamilies.domain.BankTransaction;

public interface BankTransactionDao {

    @SqlUpdate("INSERT INTO bank_transactions " +
               "(transaction_id, account_id, timestamp, amount, transaction_type, " +
               "description, customer_notes, internal_notes, attachments, audit_trail) " +
               "VALUES (:transactionId, :accountId, :timestamp, :amount, :transactionType, " +
               ":description, :customerNotes, :internalNotes, :attachments, :auditTrail)")
    void insertBankTransaction(@BindBean BankTransaction bankTransaction);

    @SqlUpdate("UPDATE bank_transactions SET customer_notes = CONCAT(coalesce(customer_notes, ''), :newNote) WHERE transaction_id = :transactionId")
    void addCustomerNote(@Bind("transactionId") UUID transactionId, @Bind("newNote") String newNote);


} 
