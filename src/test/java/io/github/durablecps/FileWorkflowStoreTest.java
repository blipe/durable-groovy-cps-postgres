package io.github.durablecps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.durablecps.api.ConcurrentWorkflowUpdateException;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.runtime.state.HistoryEntry;
import io.github.durablecps.runtime.state.ResumeValue;
import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.store.FileWorkflowStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileWorkflowStoreTest {
    @TempDir Path temporaryDirectory;

    @Test
    void atomicallyChecksRevision() {
        Instant now = Instant.parse("2026-08-01T00:00:00Z");
        WorkflowRecord initial = new WorkflowRecord(
                "wf",
                0,
                "test",
                1,
                "hash",
                WorkflowStatus.READY,
                new byte[] {1},
                null,
                new ResumeValue.Success(null),
                null,
                null,
                0,
                now,
                now,
                now,
                Map.of(),
                List.of(),
                List.of(new HistoryEntry(now, "STARTED", "test")));

        FileWorkflowStore store = new FileWorkflowStore(temporaryDirectory);
        store.create(initial);
        WorkflowRecord replacement = initial.next(
                WorkflowStatus.CANCELLED,
                null,
                null,
                null,
                null,
                null,
                0,
                null,
                List.of(),
                new HistoryEntry(now, "CANCELLED", "test"),
                now);
        store.replace("wf", 0, replacement);

        assertEquals(1, store.load("wf").orElseThrow().revision());
        assertThrows(ConcurrentWorkflowUpdateException.class, () -> store.replace("wf", 0, replacement));
    }
}
