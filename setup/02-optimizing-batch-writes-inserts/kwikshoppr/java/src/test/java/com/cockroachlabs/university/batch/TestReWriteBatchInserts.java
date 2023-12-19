package com.cockroachlabs.university.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.cockroachlabs.university.batch.dao.OrdersDao;
import com.cockroachlabs.university.batch.domain.Order;
import com.google.common.collect.Lists;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestReWriteBatchInserts extends AbstractBatchTest {

    @BeforeAll
    public void setUp() {
        setupDatabasePool();
        configureJdbi();
        
        ordersDao = jdbi.onDemand(OrdersDao.class);
    }

    @BeforeEach
    public void setUpDatabase(){
        initDatabase();
    }

    @AfterAll
    public void tearDown() {
        tearDownDatabase();
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    public void testInsertSingleOrder() {
        ordersDao = jdbi.onDemand(OrdersDao.class);

        Timer.timeExecution("testInsertSingleOrder with No Batch: ",
        () -> {
            ordersDao.insertOrder(createNewOrder());
        });
    }

    @ParameterizedTest
    @org.junit.jupiter.api.Order(2)
    @ValueSource(ints = { 2000, 3000, 10000})
    public void testInsertOrders_WithSingleBatch(int batchSize){
        final List<Order> orders = new ArrayList<Order>();

        IntStream.range(0, batchSize).forEach( i -> {
            orders.add(createNewOrder());
        });

        Timer.timeExecution("testInsertOrders_WithSingleBatch with batch size : " + batchSize,
        () -> {
            ordersDao.bulkInsert(orders);
        });
    }

    @ParameterizedTest
    @org.junit.jupiter.api.Order(3)
    @ValueSource(ints = { 2000, 3000, 10000})
    public void testInsertOrders_WithParallelPartitionedBatch(int batchSize){
        final List<Order> orders = new ArrayList<Order>();

        IntStream.range(0, batchSize).forEach( i -> {
            orders.add(createNewOrder());
        });

        Timer.timeExecution("testInsertOrders_WithParallelPartitionedBatch with batch size : " + batchSize,
        () -> {
            Lists.partition(orders, PARTITION_SIZE).parallelStream().forEach( batch -> {
                ordersDao.bulkInsert(batch);
            });
        });  
    }
}
