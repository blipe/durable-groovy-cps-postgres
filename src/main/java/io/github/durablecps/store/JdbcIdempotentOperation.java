package io.github.durablecps.store;

import java.io.Serializable;
import java.sql.Connection;

/** Business operation that must perform all database effects through the supplied transaction. */
@FunctionalInterface
public interface JdbcIdempotentOperation<T extends Serializable> {
    T execute(Connection connection) throws Exception;
}
