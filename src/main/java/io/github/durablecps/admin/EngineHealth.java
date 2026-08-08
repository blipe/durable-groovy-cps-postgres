package io.github.durablecps.admin;

import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.store.SchemaStatus;
import java.time.Instant;
import java.util.Map;

public record EngineHealth(
        HealthStatus status,
        EngineLifecycleState lifecycle,
        boolean open,
        boolean storeReachable,
        Instant checkedAt,
        Map<WorkflowStatus, Long> workflowsByStatus,
        int activeStarts,
        int activePolls,
        int activeDrivers,
        int inFlightSteps,
        int activeMaintenance,
        SchemaStatus schema,
        String message) {
    public EngineHealth {
        if (activeMaintenance < 0) throw new IllegalArgumentException("activeMaintenance must be >= 0");
        workflowsByStatus = Map.copyOf(workflowsByStatus == null ? Map.of() : workflowsByStatus);
        message = message == null ? "" : message;
    }

    public EngineHealth(
            HealthStatus status,
            EngineLifecycleState lifecycle,
            boolean open,
            boolean storeReachable,
            Instant checkedAt,
            Map<WorkflowStatus, Long> workflowsByStatus,
            int activeStarts,
            int activePolls,
            int activeDrivers,
            int inFlightSteps,
            SchemaStatus schema,
            String message) {
        this(status, lifecycle, open, storeReachable, checkedAt, workflowsByStatus,
                activeStarts, activePolls, activeDrivers, inFlightSteps, 0, schema, message);
    }
}
