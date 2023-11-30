package com.cockroachlabs.university.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.cockroachlabs.university.batch.dao.OrdersDao;
import com.cockroachlabs.university.batch.domain.Order;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AbstractBatchTest {
    protected static String dbUrl;
    protected static String dbUsername;
    protected static String dbPassword;

    protected static Jdbi jdbi;
    protected static HikariDataSource ds;

    protected OrdersDao ordersDao;

    protected static int PARTITION_SIZE = 1000;

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

        jdbi.registerRowMapper(Order.class,
            (rs, ctx) -> {
                return Order.builder()
                .id(UUID.fromString(rs.getString("id")))
                .cart_id(UUID.fromString(rs.getString("cart_id")))
                .status(rs.getString("status")).build();
            });
    }

    protected static void initDatabase() {
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE IF NOT EXISTS orders (id UUID PRIMARY KEY DEFAULT gen_random_uuid (), cart_id UUID, status STRING)");
            handle.execute("DELETE FROM orders");

        });
    }

    protected static void tearDownDatabase() {
        jdbi.useHandle(handle -> {
            handle.execute("DROP TABLE orders");
        });

        // Close the connection pool
        ds.close();
    }

    protected void loadOrdersTable(int batchSize){
        final List<Order> orders = new ArrayList<Order>();

        IntStream.range(0, batchSize).forEach( i -> {
            orders.add(createNewOrder());
        });

        ordersDao.bulkInsert(orders);
    }

    protected Order createNewOrder(){ 
        return Order.builder().id(UUID.randomUUID()).cart_id(UUID.randomUUID()).status("new").build();  
    }

    protected Order updateOrderStatus(Order order) {
        return Order.builder().id(order.getId()).cart_id(order.getCart_id()).status("processed").build();
    }

   

    



    
}
