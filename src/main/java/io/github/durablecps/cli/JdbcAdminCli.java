package io.github.durablecps.cli;

import io.github.durablecps.admin.ArchiveRequest;
import io.github.durablecps.admin.ArchiveResult;
import io.github.durablecps.admin.StuckStep;
import io.github.durablecps.admin.StuckWorkReport;
import io.github.durablecps.admin.StuckWorkflow;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.store.JdbcWorkflowStore;
import io.github.durablecps.store.SchemaStatus;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Small dependency-free operational CLI for the PostgreSQL store.
 *
 * <p>Credentials come from {@code DURABLE_CPS_JDBC_URL}, {@code DURABLE_CPS_JDBC_USER}, and
 * {@code DURABLE_CPS_JDBC_PASSWORD}. It intentionally does not start workflow execution.</p>
 */
public final class JdbcAdminCli {
    private JdbcAdminCli() {}

    public static void main(String[] args) {
        int code = run(args, System.getenv(), System.out, System.err);
        if (code != 0) System.exit(code);
    }

    static int run(String[] args, Map<String, String> environment, PrintStream out, PrintStream err) {
        Objects.requireNonNull(args, "args");
        try {
            String url = requireEnvironment(environment, "DURABLE_CPS_JDBC_URL");
            String user = environment.getOrDefault("DURABLE_CPS_JDBC_USER", "");
            String password = environment.getOrDefault("DURABLE_CPS_JDBC_PASSWORD", "");
            if (args.length == 0) {
                usage(err);
                return 2;
            }
            JdbcWorkflowStore store = new JdbcWorkflowStore(new DriverManagerDataSource(url, user, password));
            switch (args[0]) {
                case "schema" -> printSchema(out, store.schemaStatus());
                case "health" -> printHealth(out, store);
                case "limits" -> printMap(out, store.stepConcurrencyLimits());
                case "stuck" -> {
                    long seconds = positiveLong(args, 1, 300L, "overdue seconds");
                    int limit = positiveInt(args, 2, 100, "limit");
                    printStuck(out, store.scanStuckWork(Duration.ofSeconds(seconds), limit));
                }
                case "archive" -> {
                    long seconds = positiveLong(args, 1, 30L * 24L * 3600L, "terminal age seconds");
                    int limit = positiveInt(args, 2, 1_000, "limit");
                    ArchiveResult result = store.archive(new ArchiveRequest(
                            java.util.Set.of(
                                    WorkflowStatus.COMPLETED,
                                    WorkflowStatus.FAILED,
                                    WorkflowStatus.CANCELLED),
                            store.currentTime().minusSeconds(seconds),
                            limit));
                    out.println("{\"archived\":" + result.count()
                            + ",\"completedAt\":" + quote(result.completedAt()) + "}");
                }
                default -> {
                    usage(err);
                    return 2;
                }
            }
            return 0;
        } catch (Throwable failure) {
            err.println(failure.getClass().getSimpleName() + ": " + failure.getMessage());
            return 1;
        }
    }

    private static void printSchema(PrintStream out, SchemaStatus status) {
        out.println("{\"currentVersion\":" + status.currentVersion()
                + ",\"targetVersion\":" + status.targetVersion()
                + ",\"compatible\":" + status.compatible()
                + ",\"pending\":" + integerArray(status.pendingVersions())
                + ",\"message\":" + quote(status.message()) + "}");
    }

    private static void printHealth(PrintStream out, JdbcWorkflowStore store) {
        store.checkHealth();
        SchemaStatus schema = store.schemaStatus();
        StringJoiner counts = new StringJoiner(",", "{", "}");
        store.countByStatus().forEach((status, count) -> counts.add(quote(status.name()) + ":" + count));
        out.println("{\"storeReachable\":true,\"schemaCompatible\":" + schema.compatible()
                + ",\"workflowsByStatus\":" + counts + "}");
    }

    private static void printMap(PrintStream out, Map<String, Integer> values) {
        StringJoiner result = new StringJoiner(",", "{", "}");
        values.forEach((key, value) -> result.add(quote(key) + ":" + value));
        out.println(result);
    }

    private static void printStuck(PrintStream out, StuckWorkReport report) {
        List<String> workflows = new ArrayList<>();
        for (StuckWorkflow workflow : report.workflows()) {
            workflows.add("{\"workflowId\":" + quote(workflow.workflowId())
                    + ",\"status\":" + quote(workflow.status().name())
                    + ",\"waitKind\":" + quote(workflow.waitKind())
                    + ",\"dueAt\":" + quote(workflow.dueAt())
                    + ",\"updatedAt\":" + quote(workflow.updatedAt())
                    + ",\"reason\":" + quote(workflow.reason()) + "}");
        }
        List<String> steps = new ArrayList<>();
        for (StuckStep step : report.steps()) {
            steps.add("{\"invocationId\":" + quote(step.invocationId())
                    + ",\"workflowId\":" + quote(step.workflowId())
                    + ",\"stepName\":" + quote(step.stepName())
                    + ",\"status\":" + quote(step.status().name())
                    + ",\"attempt\":" + step.attempt()
                    + ",\"reason\":" + quote(step.reason()) + "}");
        }
        out.println("{\"checkedAt\":" + quote(report.checkedAt())
                + ",\"overdueSeconds\":" + report.overdueBy().toSeconds()
                + ",\"truncated\":" + report.truncated()
                + ",\"workflows\":[" + String.join(",", workflows) + "]"
                + ",\"steps\":[" + String.join(",", steps) + "]}");
    }

    private static String integerArray(List<Integer> values) {
        return "[" + values.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(",")) + "]";
    }

    private static String quote(Object value) {
        if (value == null) return "null";
        String text = value instanceof Instant instant ? instant.toString() : value.toString();
        StringBuilder escaped = new StringBuilder(text.length() + 2).append('"');
        for (int index = 0; index < text.length(); index++) {
            char c = text.charAt(index);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) escaped.append(String.format("\\u%04x", (int) c));
                    else escaped.append(c);
                }
            }
        }
        return escaped.append('"').toString();
    }

    private static long positiveLong(String[] args, int index, long defaultValue, String name) {
        long value = args.length > index ? Long.parseLong(args[index]) : defaultValue;
        if (value < 1) throw new IllegalArgumentException(name + " must be >= 1");
        return value;
    }

    private static int positiveInt(String[] args, int index, int defaultValue, String name) {
        long value = positiveLong(args, index, defaultValue, name);
        if (value > Integer.MAX_VALUE) throw new IllegalArgumentException(name + " is too large");
        return (int) value;
    }

    private static String requireEnvironment(Map<String, String> environment, String name) {
        String value = environment.get(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
        return value;
    }

    private static void usage(PrintStream out) {
        out.println("usage: JdbcAdminCli <health|schema|limits|stuck [overdue-seconds] [limit]|archive [age-seconds] [limit]>");
    }

    private record DriverManagerDataSource(String url, String user, String password) implements DataSource {
        @Override public Connection getConnection() throws SQLException { return open(user, password); }
        @Override public Connection getConnection(String username, String pwd) throws SQLException { return open(username, pwd); }
        private Connection open(String username, String pwd) throws SQLException {
            Properties properties = new Properties();
            if (username != null && !username.isEmpty()) properties.setProperty("user", username);
            if (pwd != null && !pwd.isEmpty()) properties.setProperty("password", pwd);
            return DriverManager.getConnection(url, properties);
        }
        @Override public java.io.PrintWriter getLogWriter() throws SQLException { return DriverManager.getLogWriter(); }
        @Override public void setLogWriter(java.io.PrintWriter out) throws SQLException { DriverManager.setLogWriter(out); }
        @Override public void setLoginTimeout(int seconds) throws SQLException { DriverManager.setLoginTimeout(seconds); }
        @Override public int getLoginTimeout() throws SQLException { return DriverManager.getLoginTimeout(); }
        @Override public Logger getParentLogger() { return Logger.getLogger("io.github.durablecps"); }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("not a wrapper for " + iface.getName());
        }
        @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
    }
}
