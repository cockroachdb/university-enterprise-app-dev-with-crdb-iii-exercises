package com.cockroachlabs.university.batch.dao;

import java.util.List;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.cockroachlabs.university.batch.domain.Order;

public interface OrdersDao {

    @SqlBatch("INSERT INTO orders(id, cart_id, status) values (:id, :cart_id, :status)")
    void bulkInsert(@BindBean List<Order> orders);

    @SqlBatch("UPDATE orders SET status=:status WHERE id=:id")
    void bulkUpdate(@BindBean List<Order> orders);

    @SqlUpdate("INSERT INTO orders(id, cart_id, status) values (:id, :cart_id, :status)")
    int insertOrder(@BindBean Order order);

}
