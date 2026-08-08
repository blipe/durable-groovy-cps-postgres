package io.github.durablecps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.durablecps.api.DurableCommand;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.runtime.state.HistoryEntry;
import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.store.InMemoryWorkflowStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Opt-in capacity smoke; run with mvn -Pload-tests test. */
@Tag("load")
class ScaleSmokeTest {
    @Test
    void storesAndScansOneHundredThousandWaitingWorkflows() {
        InMemoryWorkflowStore store = new InMemoryWorkflowStore();
        Instant now = Instant.parse("2026-08-01T00:00:00Z");
        int count = 100_000;
        for (int i = 0; i < count; i++) {
            String id = "load-" + i;
            store.create(new WorkflowRecord(
                    id,
                    0L,
                    "load-definition",
                    1,
                    "hash",
                    WorkflowStatus.WAITING,
                    new byte[] {1},
                    new DurableCommand.Sleep(1L, Duration.ofDays(1)),
                    null,
                    null,
                    null,
                    0,
                    now.plus(Duration.ofDays(1)),
                    now,
                    now,
                    Map.of(),
                    List.of(),
                    List.of(new HistoryEntry(now, "STARTED", id))));
        }
        assertEquals(count, store.list().size());
    }
}
