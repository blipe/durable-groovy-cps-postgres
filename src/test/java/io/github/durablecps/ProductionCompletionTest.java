package io.github.durablecps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.durablecps.admin.ArchiveRequest;
import io.github.durablecps.admin.ArchiveResult;
import io.github.durablecps.admin.ArchivedWorkflow;
import io.github.durablecps.admin.ArchivedWorkflowSummary;
import io.github.durablecps.admin.RetentionPolicy;
import io.github.durablecps.runtime.AesGcmObjectCodec;
import io.github.durablecps.runtime.EncryptionKey;
import io.github.durablecps.runtime.JavaObjectCodec;
import io.github.durablecps.runtime.MapEncryptionKeyProvider;
import io.github.durablecps.runtime.SnapshotIntegrityException;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.store.ArchiveCapableWorkflowStore;
import io.github.durablecps.store.InMemoryWorkflowStore;
import io.github.durablecps.store.WorkflowStore;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class ProductionCompletionTest {
    @Test
    void aesGcmSupportsRotationTamperDetectionAndPlaintextRollingUpgrade() {
        EncryptionKey oldKey = new EncryptionKey(
                "2026-01", new SecretKeySpec(new byte[32], "AES"));
        byte[] nextBytes = new byte[32];
        java.util.Arrays.fill(nextBytes, (byte) 9);
        EncryptionKey nextKey = new EncryptionKey(
                "2026-08", new SecretKeySpec(nextBytes, "AES"));

        AesGcmObjectCodec oldCodec = new AesGcmObjectCodec(
                new JavaObjectCodec(), new MapEncryptionKeyProvider(oldKey.id(), List.of(oldKey)));
        byte[] oldSnapshot = oldCodec.encode("old-value");

        AesGcmObjectCodec rotated = new AesGcmObjectCodec(
                new JavaObjectCodec(),
                new MapEncryptionKeyProvider(nextKey.id(), List.of(oldKey, nextKey)));
        assertEquals("old-value", rotated.decode(oldSnapshot, String.class, getClass().getClassLoader()));
        assertTrue(rotated.inspect(oldSnapshot).codecId().contains(oldKey.id()));

        byte[] current = rotated.encode("new-value");
        assertEquals("new-value", rotated.decode(current, String.class, getClass().getClassLoader()));
        current[current.length - 1] ^= 1;
        assertThrows(SnapshotIntegrityException.class,
                () -> rotated.decode(current, String.class, getClass().getClassLoader()));

        byte[] plaintextEnvelope = new JavaObjectCodec().encode("rolling-upgrade");
        assertEquals("rolling-upgrade",
                rotated.decode(plaintextEnvelope, String.class, getClass().getClassLoader()));
    }

    @Test
    void stepConcurrencyRequiresDatabaseCapableStore() {
        assertThrows(IllegalStateException.class, () -> DurableWorkflowEngine.builder(new InMemoryWorkflowStore())
                .stepConcurrencyLimit("payment", 4)
                .automaticPolling(false)
                .build());
    }

    @Test
    void retentionRunsOnDedicatedMaintenanceScheduler() throws Exception {
        RecordingArchiveStore store = new RecordingArchiveStore();
        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
                .automaticPolling(false)
                .retentionPolicy(new RetentionPolicy(
                        Duration.ZERO,
                        Duration.ofMillis(10),
                        25,
                        java.util.Set.of(io.github.durablecps.api.WorkflowStatus.COMPLETED)))
                .build()) {
            assertTrue(store.called.await(2, TimeUnit.SECONDS));
            assertEquals(25, store.lastRequest.limit());
            assertTrue(engine.administration().health().activeMaintenance() >= 0);
        }
    }

    private static final class RecordingArchiveStore implements WorkflowStore, ArchiveCapableWorkflowStore {
        private final InMemoryWorkflowStore delegate = new InMemoryWorkflowStore();
        private final CountDownLatch called = new CountDownLatch(1);
        private volatile ArchiveRequest lastRequest;

        @Override
        public ArchiveResult archive(ArchiveRequest request) {
            lastRequest = request;
            called.countDown();
            return new ArchiveResult(Instant.now(), List.of());
        }

        @Override public Optional<ArchivedWorkflow> loadArchived(String archiveId) { return Optional.empty(); }
        @Override public List<ArchivedWorkflowSummary> listArchived(int limit, int offset) { return List.of(); }
        @Override public WorkflowRecord create(WorkflowRecord initial) { return delegate.create(initial); }
        @Override public Optional<WorkflowRecord> load(String workflowId) { return delegate.load(workflowId); }
        @Override public WorkflowRecord replace(String id, long revision, WorkflowRecord replacement) {
            return delegate.replace(id, revision, replacement);
        }
        @Override public Collection<WorkflowRecord> list() { return delegate.list(); }
    }
}
