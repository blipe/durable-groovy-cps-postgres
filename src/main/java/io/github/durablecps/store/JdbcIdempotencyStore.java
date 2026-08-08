package io.github.durablecps.store;

import io.github.durablecps.runtime.JavaObjectCodec;
import io.github.durablecps.runtime.ObjectCodec;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * PostgreSQL idempotency helper for step handlers whose side effects live in the same database.
 *
 * <p>The reservation row, business mutations performed through the supplied connection, and cached
 * result are committed in one transaction. Concurrent calls for the same invocation ID wait on the
 * unique key and then reuse the committed result.</p>
 */
public final class JdbcIdempotencyStore {
    private final DataSource dataSource;
    private final ObjectCodec codec;
    private final ClassLoader classLoader;

    public JdbcIdempotencyStore(DataSource dataSource) {
        this(dataSource, new JavaObjectCodec(), JdbcIdempotencyStore.class.getClassLoader());
    }

    public JdbcIdempotencyStore(DataSource dataSource, ObjectCodec codec, ClassLoader classLoader) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
    }

    public <T extends Serializable> T executeOnce(
            String invocationId,
            Class<T> resultType,
            JdbcIdempotentOperation<T> operation) {
        Objects.requireNonNull(invocationId, "invocationId");
        Objects.requireNonNull(resultType, "resultType");
        Objects.requireNonNull(operation, "operation");
        if (invocationId.isBlank()) throw new IllegalArgumentException("invocationId must not be blank");

        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                Instant now = databaseNow(connection);
                boolean owner = reserve(connection, invocationId, now);
                if (!owner) {
                    T result = readCompleted(connection, invocationId, resultType);
                    connection.commit();
                    return result;
                }

                T result = operation.execute(guardedConnection(connection));
                byte[] bytes = result == null ? null : codec.encode(result);
                complete(connection, invocationId, result == null, bytes, databaseNow(connection));
                connection.commit();
                return result;
            } catch (Throwable failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                if (failure instanceof Error error) throw error;
                if (failure instanceof RuntimeException runtime) throw runtime;
                throw new IdempotencyOperationException(
                        "idempotent operation failed for " + invocationId, failure);
            } finally {
                try {
                    connection.setAutoCommit(originalAutoCommit);
                } catch (SQLException ignored) {
                    // Connection is closing.
                }
            }
        } catch (SQLException failure) {
            throw new IdempotencyOperationException(
                    "idempotency transaction failed for " + invocationId, failure);
        }
    }


    private static Connection guardedConnection(Connection delegate) {
        return (Connection) Proxy.newProxyInstance(
                JdbcIdempotencyStore.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, arguments) -> {
                    String name = method.getName();
                    if (name.equals("commit")
                            || name.equals("rollback")
                            || name.equals("close")
                            || name.equals("abort")
                            || name.equals("setAutoCommit")) {
                        throw new SQLException(
                                "transaction control is owned by JdbcIdempotencyStore: " + name);
                    }
                    if (name.equals("unwrap") && arguments != null && arguments.length == 1) {
                        Class<?> requested = (Class<?>) arguments[0];
                        if (requested.isInstance(proxy)) return proxy;
                    }
                    if (name.equals("isWrapperFor") && arguments != null && arguments.length == 1) {
                        Class<?> requested = (Class<?>) arguments[0];
                        if (requested.isInstance(proxy)) return true;
                    }
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private boolean reserve(Connection connection, String invocationId, Instant now) throws SQLException {
        String sql = """
                insert into durable_idempotency (
                    invocation_id, completed, result_is_null, result_blob, created_at, completed_at
                ) values (?, false, false, null, ?, null)
                on conflict (invocation_id) do nothing
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, invocationId);
            statement.setTimestamp(2, Timestamp.from(now));
            return statement.executeUpdate() == 1;
        }
    }

    private <T extends Serializable> T readCompleted(
            Connection connection, String invocationId, Class<T> resultType) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select completed, result_is_null, result_blob
                from durable_idempotency where invocation_id = ?
                """)) {
            statement.setString(1, invocationId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException(
                            "idempotency reservation disappeared: " + invocationId);
                }
                if (!row.getBoolean(1)) {
                    throw new IllegalStateException(
                            "incomplete idempotency row was committed: " + invocationId);
                }
                if (row.getBoolean(2)) return null;
                byte[] bytes = row.getBytes(3);
                if (bytes == null) {
                    throw new IllegalStateException(
                            "completed idempotency row has no result: " + invocationId);
                }
                return codec.decode(bytes, resultType, classLoader);
            }
        }
    }

    private void complete(
            Connection connection,
            String invocationId,
            boolean resultIsNull,
            byte[] result,
            Instant completedAt)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                update durable_idempotency
                set completed = true, result_is_null = ?, result_blob = ?, completed_at = ?
                where invocation_id = ? and completed = false
                """)) {
            statement.setBoolean(1, resultIsNull);
            statement.setBytes(2, result);
            statement.setTimestamp(3, Timestamp.from(completedAt));
            statement.setString(4, invocationId);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException(
                        "could not complete idempotency reservation: " + invocationId);
            }
        }
    }

    private static Instant databaseNow(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select current_timestamp");
                ResultSet row = statement.executeQuery()) {
            row.next();
            return row.getTimestamp(1).toInstant();
        }
    }
}
