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

                        String sql = "UPDATE orders SET status=data_table.new_status "
                                + "FROM (select unnest(:id) as id, unnest(:new_status) as new_status) as data_table "
                                + "WHERE orders.id=data_table.id";

                        PreparedBatch preparedBatch = handle.prepareBatch(sql);

                        List<String> status = new ArrayList<>();
                        List<UUID> ids = new ArrayList<>();

                        ordersToUpdate.forEach(
                                o -> {
                                    status.add("processed");
                                    ids.add(o.getId());
                                });

                        preparedBatch.bindArray("id", UUID.class, ids.toArray())
                                .bindArray("new_status", String.class, status.toArray());

                        preparedBatch.execute();
                    });
                });
    }

    @ParameterizedTest
    @org.junit.jupiter.api.Order(4)
    @ValueSource(ints = {1000, 2000, 10000 })
    public void testInsertOrders_WithParallelPartitionedBatch(int batchSize) {

        loadOrdersTable(batchSize);
        log.info("testInsertOrders_WithParallelPartitionedBatch loaded order data");

        final List<Order> ordersToUpdate = new ArrayList<>(); 
        
        ordersDao.getOrdersAsStream(
                stream -> stream.forEach(ordersToUpdate::add));
        log.info("testInsertOrders_WithParallelPartitionedBatch loaded orders to update list");

        String sql = "UPDATE orders SET status=data_table.new_status "
                                    + "FROM (select unnest(:id) as id, unnest(:new_status) as new_status) as data_table "
                                    + "WHERE orders.id=data_table.id";


        Timer.timeExecution("testUpdateOrders_ArrayBindingWithParallelPartitionedBatch with batch size: " + batchSize,
                () -> {

                    Lists.partition(ordersToUpdate, PARTITION_SIZE).parallelStream().forEach(batch -> {

                        List<String> status = new ArrayList<>();
                        List<UUID> ids = new ArrayList<>();

                        jdbi.useHandle(handle -> {
                            PreparedBatch preparedBatch = handle.prepareBatch(sql); 
                            
                            batch.forEach(
                                    o -> {
                                        status.add("processed");
                                        ids.add(o.getId());
                                    });

                            preparedBatch.bindArray("id", UUID.class, ids.toArray())
                                    .bindArray("new_status", String.class, status.toArray());

                            preparedBatch.execute();
                        });
                    });

                });

    }
    
}
