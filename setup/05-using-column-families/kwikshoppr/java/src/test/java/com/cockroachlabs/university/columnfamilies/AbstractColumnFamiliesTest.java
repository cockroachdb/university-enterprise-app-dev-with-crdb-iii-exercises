package com.cockroachlabs.university.columnfamilies;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.cockroachlabs.university.columnfamilies.dao.BankTransactionDao;
import com.cockroachlabs.university.columnfamilies.domain.BankTransaction;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractColumnFamiliesTest {
    private static String dbUrl;
    private static String dbUsername;
    protected static String dbPassword;

    protected static Jdbi jdbi;
    private static HikariDataSource ds;

    protected BankTransactionDao bankTransactionDao;

   private static final Random random = new Random();
   private static final byte[] attachment = getAttachment();

   protected static void setupDatabasePool() {
        try {
            File configFile = new File("target/application.properties");

            Properties properties = new Properties();
            try (FileInputStream input = new FileInputStream(configFile)) {
                properties.load(input);
            }

            dbUrl = properties.getProperty("db.url");
            log.info("USING CONNECTION STRING: " + dbUrl);
            dbUsername = properties.getProperty("db.username");
            dbPassword = properties.getProperty("db.password");

        } catch (IOException exception) {
            log.error(exception.getMessage(), exception);
            System.exit(1);
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUsername);
        config.setPassword(dbPassword);
        config.setMaximumPoolSize(20);
        ds = new HikariDataSource(config);
        
    }

    protected static void configureJdbi() {
        // Wrap the HikariDataSource in JDBI:
        jdbi = Jdbi.create(ds);
        jdbi.installPlugin(new SqlObjectPlugin());

        jdbi.registerRowMapper(BankTransaction.class, new RowMapper<BankTransaction>() {
        @Override
        public BankTransaction map(ResultSet rs, StatementContext ctx) throws SQLException {
            return BankTransaction.builder()
                .transactionId(UUID.fromString(rs.getString("transaction_id")))
                .accountId(UUID.fromString(rs.getString("account_id")))
                .timestamp(rs.getObject("timestamp", LocalDateTime.class))
                .amount(rs.getBigDecimal("amount"))
                .transactionType(rs.getString("transaction_type"))
                .description(rs.getString("description"))
                .customerNotes(rs.getString("customer_notes"))
                .internalNotes(rs.getString("internal_notes"))
                .attachments(rs.getBytes("attachments"))
                .auditTrail(rs.getString("audit_trail"))
                .build();
            }
        });
    }


    protected static void tearDownDatabase() {
        jdbi.useHandle(handle -> {
            handle.execute("DROP TABLE bank_transactions");
        });

        // Close the connection pool
        ds.close();
    }

    public static BankTransaction generateTransaction() {
        return BankTransaction.builder()
                .transactionId(UUID.randomUUID())
                .accountId(UUID.randomUUID())
                .timestamp(LocalDateTime.now())
                .amount(new BigDecimal(Math.abs(random.nextDouble() * 10000)))
                .transactionType(generateRandomTransactionType())
                .description("Random Transaction Description")
                .customerNotes("Sample Customer Note")
                .internalNotes("Sample Internal Note")
                .auditTrail("Sample Audit Trail")
                .attachments(attachment)
                .build();

    }

    private static String generateRandomTransactionType() {
        String[] types = {"Deposit", "Withdrawal", "Transfer", "Payment"};
        return types[random.nextInt(types.length)];
    }

    private static byte [] getAttachment(){

        try {
            URL resourceUrl = AbstractColumnFamiliesTest.class.getClassLoader().getResource("big-file.jpg");
            if (resourceUrl == null) {
                throw new IOException("Resource file not found: " );
            }
            Path path = Paths.get(resourceUrl.toURI());
            return Files.readAllBytes(path);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
    }
 
}
