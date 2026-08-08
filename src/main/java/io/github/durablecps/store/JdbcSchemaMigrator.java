package io.github.durablecps.store;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;

/** Ordered, checksummed schema migrations for {@link JdbcWorkflowStore}. */
final class JdbcSchemaMigrator {
    private static final long POSTGRES_ADVISORY_LOCK = 0x44555241424c4553L; // "DURABLES"
    private static final String HISTORY_TABLE = "durable_schema_history";
    private static final List<Migration> MIGRATIONS = List.of(
            load(1, "baseline", "/io/github/durablecps/store/migration/V001__baseline.sql"),
            load(2, "operational indexes", "/io/github/durablecps/store/migration/V002__operational_indexes.sql"),
            load(3, "step concurrency", "/io/github/durablecps/store/migration/V003__step_concurrency.sql"));

    private final DataSource dataSource;

    JdbcSchemaMigrator(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    SchemaStatus status() {
        try (Connection connection = dataSource.getConnection()) {
            List<AppliedSchemaMigration> applied = historyExists(connection)
                    ? readApplied(connection)
                    : List.of();
            return statusFrom(applied);
        } catch (SQLException e) {
            throw new SchemaMigrationException("cannot inspect durable workflow schema", e);
        }
    }

    SchemaStatus migrate() {
        try (Connection connection = dataSource.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                createHistoryTable(connection);
                acquireMigrationLock(connection);
                Map<Integer, AppliedSchemaMigration> applied = byVersion(readApplied(connection));
                validateApplied(applied);
                for (Migration migration : MIGRATIONS) {
                    if (applied.containsKey(migration.version())) continue;
                    for (String sql : splitStatements(migration.sql())) {
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(sql);
                        }
                    }
                    try (PreparedStatement statement = connection.prepareStatement("""
                            insert into durable_schema_history
                                (version, description, checksum, installed_at)
                            values (?, ?, ?, current_timestamp)
                            """)) {
                        statement.setInt(1, migration.version());
                        statement.setString(2, migration.description());
                        statement.setString(3, migration.checksum());
                        statement.executeUpdate();
                    }
                }
                connection.commit();
                connection.setAutoCommit(autoCommit);
                return status();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                try {
                    connection.setAutoCommit(autoCommit);
                } catch (SQLException suppressed) {
                    failure.addSuppressed(suppressed);
                }
                if (failure instanceof SchemaMigrationException migrationFailure) {
                    throw migrationFailure;
                }
                throw new SchemaMigrationException("cannot migrate durable workflow schema", failure);
            }
        } catch (SQLException e) {
            throw new SchemaMigrationException("cannot open database for schema migration", e);
        }
    }

    private static void createHistoryTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists durable_schema_history (
                        version integer primary key,
                        description varchar(200) not null,
                        checksum varchar(64) not null,
                        installed_at timestamp not null
                    )
                    """);
        }
    }

    private static void acquireMigrationLock(Connection connection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String product = metadata == null ? "" : metadata.getDatabaseProductName();
        if (product != null && product.toLowerCase(java.util.Locale.ROOT).contains("postgresql")) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "select pg_advisory_xact_lock(?)")) {
                statement.setLong(1, POSTGRES_ADVISORY_LOCK);
                statement.execute();
            }
        } else {
            // The history row lock serializes established schemas on portable JDBC databases.
            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("select version from durable_schema_history for update").close();
            }
        }
    }

    private static boolean historyExists(Connection connection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        for (String name : List.of(HISTORY_TABLE, HISTORY_TABLE.toUpperCase(java.util.Locale.ROOT))) {
            try (ResultSet rows = metadata.getTables(null, null, name, new String[] {"TABLE"})) {
                if (rows.next()) return true;
            }
        }
        return false;
    }

    private static List<AppliedSchemaMigration> readApplied(Connection connection) throws SQLException {
        List<AppliedSchemaMigration> result = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement("""
                        select version, description, checksum, installed_at
                        from durable_schema_history
                        order by version
                        """);
                ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                Timestamp installed = rows.getTimestamp(4);
                result.add(new AppliedSchemaMigration(
                        rows.getInt(1),
                        rows.getString(2),
                        rows.getString(3),
                        installed == null ? Instant.EPOCH : installed.toInstant()));
            }
        }
        return List.copyOf(result);
    }

    private static Map<Integer, AppliedSchemaMigration> byVersion(List<AppliedSchemaMigration> applied) {
        Map<Integer, AppliedSchemaMigration> result = new LinkedHashMap<>();
        for (AppliedSchemaMigration migration : applied) result.put(migration.version(), migration);
        return result;
    }

    private static void validateApplied(Map<Integer, AppliedSchemaMigration> applied) {
        int target = targetVersion();
        for (AppliedSchemaMigration installed : applied.values()) {
            if (installed.version() > target) {
                throw new SchemaMigrationException(
                        "database schema version " + installed.version()
                                + " is newer than runtime target " + target);
            }
            Migration expected = MIGRATIONS.stream()
                    .filter(candidate -> candidate.version() == installed.version())
                    .findFirst()
                    .orElseThrow(() -> new SchemaMigrationException(
                            "runtime has no migration definition for installed version " + installed.version()));
            if (!expected.checksum().equals(installed.checksum())) {
                throw new SchemaMigrationException(
                        "schema migration checksum mismatch for version " + installed.version()
                                + ": database=" + installed.checksum()
                                + " runtime=" + expected.checksum());
            }
        }
    }

    private static SchemaStatus statusFrom(List<AppliedSchemaMigration> applied) {
        Map<Integer, AppliedSchemaMigration> byVersion = byVersion(applied);
        try {
            validateApplied(byVersion);
        } catch (SchemaMigrationException incompatible) {
            int current = applied.stream().mapToInt(AppliedSchemaMigration::version).max().orElse(0);
            return new SchemaStatus(
                    current, targetVersion(), false, applied, pending(byVersion), incompatible.getMessage());
        }
        int current = applied.stream().mapToInt(AppliedSchemaMigration::version).max().orElse(0);
        List<Integer> pending = pending(byVersion);
        boolean compatible = pending.isEmpty() && current == targetVersion();
        return new SchemaStatus(
                current,
                targetVersion(),
                compatible,
                applied,
                pending,
                compatible
                        ? "schema is current at version " + current
                        : "schema requires migration from version " + current
                                + " to " + targetVersion());
    }

    private static List<Integer> pending(Map<Integer, AppliedSchemaMigration> applied) {
        return MIGRATIONS.stream()
                .map(Migration::version)
                .filter(version -> !applied.containsKey(version))
                .sorted()
                .toList();
    }

    private static int targetVersion() {
        return MIGRATIONS.stream().mapToInt(Migration::version).max().orElse(0);
    }

    private static Migration load(int version, String description, String resource) {
        try (InputStream stream = JdbcSchemaMigrator.class.getResourceAsStream(resource)) {
            if (stream == null) throw new IllegalStateException("missing schema migration: " + resource);
            String sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("\r\n", "\n")
                    .replace('\r', '\n');
            return new Migration(version, description, resource, sql, sha256(sql));
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    static List<String> splitStatements(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean singleQuoted = false;
        boolean doubleQuoted = false;
        boolean lineComment = false;
        for (int index = 0; index < script.length(); index++) {
            char c = script.charAt(index);
            char next = index + 1 < script.length() ? script.charAt(index + 1) : '\0';
            if (lineComment) {
                if (c == '\n') lineComment = false;
                continue;
            }
            if (!singleQuoted && !doubleQuoted && c == '-' && next == '-') {
                lineComment = true;
                index++;
                continue;
            }
            if (c == '\'' && !doubleQuoted) {
                if (singleQuoted && next == '\'') {
                    current.append(c).append(next);
                    index++;
                    continue;
                }
                singleQuoted = !singleQuoted;
            } else if (c == '"' && !singleQuoted) {
                doubleQuoted = !doubleQuoted;
            }
            if (c == ';' && !singleQuoted && !doubleQuoted) {
                String sql = current.toString().trim();
                if (!sql.isEmpty()) statements.add(sql);
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        String sql = current.toString().trim();
        if (!sql.isEmpty()) statements.add(sql);
        return List.copyOf(statements);
    }

    private static void rollback(Connection connection, Throwable original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }

    private record Migration(
            int version,
            String description,
            String resource,
            String sql,
            String checksum) {}
}
