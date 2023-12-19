package com.cockroachlabs.university.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.jdbi.v3.core.statement.PreparedBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.cockroachlabs.university.batch.dao.OrdersDao;
import com.cockroachlabs.university.batch.domain.Order;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestArrayBindingBulkUpdates extends AbstractBatchTest {
    @BeforeAll
    public void setUp() {
        setupDatabasePool();
        configureJdbi();

        ordersDao = jdbi.onDemand(OrdersDao.class);
    }

    @BeforeEach
    public void setUpDatabase() {
        initDatabase();
    }

    @AfterAll
    public void tearDown() {
        tearDownDatabase();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1000, 2000, 10000 })
    public void testUpdateOrders_WithSingleBatch(int batchSize) {
        loadOrdersTable(batchSize);

        final List<Order> ordersToUpdate = new ArrayList<>(); 
        
        ordersDao.getOrdersAsStream(
                stream -> stream.forEach(ordersToUpdate::add));

        final List<Order> updatedOrders = ordersToUpdate
                .stream()
                .map(this::updateOrderStatus)
                .collect(Collectors.toList());

        Timer.timeExecution("testUpdateOrders_WithSingleBatch with batch size: " + batchSize,
                () -> {
                    ordersDao.bulkUpdate(updatedOrders);
                });
    }

    @ParameterizedTest
    @ValueSource(ints = { 1000, 2000, 10000 })
    public void testUpdateOrders_ArrayBindingWithSingleBatch(int batchSize) {
        loadOrdersTable(batchSize);

        final List<Order> ordersToUpdate = new ArrayList<>(); 
        
        ordersDao.getOrdersAsStream(
                stream -> stream.forEach(ordersToUpdate::add));

        Timer.timeExecution("testUpdateOrders_ArrayBindingWithSingleBatch with batch size: " + batchSize,
                () -> {
                    jdbi.useHandle(handle -> {

                        //TODO: Update SQL query string
                        String sql = "";

                        PreparedBatch preparedBatch = handle.prepareBatch(sql);

                        List<String> status = new ArrayList<>();
                        List<UUID> ids = new ArrayList<>();

                        ordersToUpdate.forEach(
                                o -> {
                                    status.add("processed");
                                    ids.add(o.getId());
                                });

                        //TODO: Bind arrays to the preparedBatch. 
                        //      Use syntax -- bindArray(String name, Type elementType, Object... array)
                        //      Tip: You can chain bind methods for example
                        //           preparedBatch.bindArray(...).bindArray(...).bindArray(...);

                        preparedBatch.execute();
                    });
                });
    }
    
}
