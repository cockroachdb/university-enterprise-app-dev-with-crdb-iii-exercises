package com.cockroachlabs.university.columnfamilies;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.cockroachlabs.university.columnfamilies.dao.BankTransactionDao;
import com.cockroachlabs.university.columnfamilies.domain.BankTransaction;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ColumnFamiliesTest extends AbstractColumnFamiliesTest {

    @BeforeAll
    public void setUp() {
        setupDatabasePool();
        configureJdbi();

        bankTransactionDao = jdbi.onDemand(BankTransactionDao.class);
    }

    @AfterAll
    public void tearDown() {
        tearDownDatabase();
    }

    private void createTable() {

        jdbi.useHandle(handle -> {
            handle.execute("DROP TABLE IF EXISTS bank_transactions");
        });

        String sql = "CREATE TABLE bank_transactions (" +
                "    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid()," +
                "    account_id UUID NOT NULL," +
                "    timestamp TIMESTAMP NOT NULL DEFAULT now()," +
                "    amount DECIMAL NOT NULL," +
                "    transaction_type STRING NOT NULL," +
                "    description STRING," +
                "    customer_notes STRING," +
                "    internal_notes STRING," +
                "    attachments BYTES," +
                "    audit_trail STRING" +
                ");";

        jdbi.useHandle(handle -> {
            handle.execute(sql);
        });
    }

    private void createTableUsingColumnFamilies() {
        jdbi.useHandle(handle -> {
            handle.execute("DROP TABLE IF EXISTS bank_transactions");
        });

        String sqlWithColumnFamilies = "CREATE TABLE bank_transactions (" +
                "    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid()," +
                "    account_id UUID NOT NULL," +
                "    timestamp TIMESTAMP NOT NULL DEFAULT now()," +
                "    amount DECIMAL NOT NULL," +
                "    transaction_type STRING NOT NULL," +
                "    description STRING," +
                "    customer_notes STRING," +
                "    internal_notes STRING," +
                "    attachments BYTES," +
                "    audit_trail STRING," +
                "    FAMILY f1 (transaction_id, account_id, timestamp, amount, transaction_type)," +
                "    FAMILY f2 (description, customer_notes)," +
                "    FAMILY f3 (internal_notes, attachments, audit_trail)" +
                ");";

        jdbi.useHandle(handle -> {
            handle.execute(sqlWithColumnFamilies);
        });
    }

    @Test
    public void testUpdateTransactionWithoutColumnFamily() {

        createTable();

        String timerLabel = 
                 "test update transaction WITHOUT column family defined";

        BankTransaction bankTransaction = generateTransaction();

        bankTransactionDao.insertBankTransaction(bankTransaction);

        Timer.timeExecution(timerLabel,
                () -> {
                    bankTransactionDao.addCustomerNote(bankTransaction.getTransactionId(),
                            "\n a new note from customer");
                });
    }

    @Test
    public void testUpdateTransactionUsingColumnFamily() {

        createTableUsingColumnFamilies();

        String timerLabel = 
                 "test update transaction WITH column families defined";

        BankTransaction bankTransaction = generateTransaction();

        bankTransactionDao.insertBankTransaction(bankTransaction);

        Timer.timeExecution(timerLabel,
                () -> {
                    bankTransactionDao.addCustomerNote(bankTransaction.getTransactionId(),
                            "\n a new note from customer");
                });
    }

}
