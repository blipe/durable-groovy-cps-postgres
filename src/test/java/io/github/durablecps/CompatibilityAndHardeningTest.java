package io.github.durablecps;

import com.acme.RestrictedValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.durablecps.admin.DefinitionPreflightException;
import io.github.durablecps.admin.DefinitionPreflightMode;
import io.github.durablecps.api.WorkflowDefinition;
import io.github.durablecps.api.WorkflowStatus;
import io.github.durablecps.runtime.DeserializationPolicy;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.runtime.JavaObjectCodec;
import io.github.durablecps.runtime.SnapshotIntegrityException;
import io.github.durablecps.runtime.state.HistoryEntry;
import io.github.durablecps.runtime.state.ResumeValue;
import io.github.durablecps.runtime.state.WorkflowRecord;
import io.github.durablecps.store.InMemoryWorkflowStore;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CompatibilityAndHardeningTest {
    @Test
    void snapshotEnvelopeIsInspectableAndChecksumProtected() {
        JavaObjectCodec codec = new JavaObjectCodec();
        byte[] encoded = codec.encode((Serializable) new ArrayList<>(List.of("value", 7)));
        var metadata = codec.inspect(encoded);
        assertTrue(metadata.enveloped());
        assertEquals("java-serialization", metadata.codecId());
        assertEquals(1, metadata.codecVersion());
        assertEquals(List.of("value", 7), codec.decode(encoded, ArrayList.class, getClass().getClassLoader()));

        encoded[encoded.length - 1] ^= 1;
        assertThrows(
                SnapshotIntegrityException.class,
                () -> codec.decode(encoded, Serializable.class, getClass().getClassLoader()));
    }

    @Test
    void legacyRawJavaSerializationStillDecodes() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject("legacy");
        }
        JavaObjectCodec codec = new JavaObjectCodec();
        assertFalse(codec.inspect(bytes.toByteArray()).enveloped());
        assertEquals("legacy", codec.decode(bytes.toByteArray(), String.class, getClass().getClassLoader()));
    }

    @Test
    void strictPolicyRejectsUnapprovedApplicationClassesAndCanAdmitThemExplicitly() {
        RestrictedValue value = new RestrictedValue("secret");
        JavaObjectCodec strict = new JavaObjectCodec();
        byte[] encoded = strict.encode(value);
        assertThrows(
                IllegalStateException.class,
                () -> strict.decode(encoded, RestrictedValue.class, getClass().getClassLoader()));

        JavaObjectCodec admitted = new JavaObjectCodec(DeserializationPolicy.builder()
                .allowPackage("java.")
                .allowPackage("io.github.durablecps.")
                .allowPackage("com.acme.")
                .build());
        assertEquals(value, admitted.decode(encoded, RestrictedValue.class, getClass().getClassLoader()));
    }

    @Test
    void historyIsBoundedButPreservesOrigin() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        List<HistoryEntry> history = new ArrayList<>();
        history.add(new HistoryEntry(now, "STARTED", "origin"));
        for (int i = 1; i <= WorkflowRecord.MAX_HISTORY_ENTRIES + 50; i++) {
            history.add(new HistoryEntry(now.plusSeconds(i), "EVENT", Integer.toString(i)));
        }
        WorkflowRecord record = ready("bounded-history", "hash", now, history);
        assertEquals(WorkflowRecord.MAX_HISTORY_ENTRIES, record.history().size());
        assertEquals("STARTED", record.history().get(0).type());
        assertEquals(Integer.toString(WorkflowRecord.MAX_HISTORY_ENTRIES + 50),
                record.history().get(record.history().size() - 1).detail());
    }

    @Test
    void startupCanFailBeforePollingWhenPersistedDefinitionIsUnavailable() {
        InMemoryWorkflowStore store = new InMemoryWorkflowStore();
        store.create(ready("missing-definition", "missing-hash", Instant.now(), List.of()));
        DefinitionPreflightException failure = assertThrows(
                DefinitionPreflightException.class,
                () -> DurableWorkflowEngine.builder(store)
                        .automaticPolling(false)
                        .definitionPreflight(DefinitionPreflightMode.FAIL)
                        .build());
        assertEquals(1L, failure.report().incompatibleWorkflows());
    }

    @Test
    void exactRegisteredSourcePassesDefinitionPreflight() throws Exception {
        String source = "return input('value')";
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(source.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        InMemoryWorkflowStore store = new InMemoryWorkflowStore();
        store.create(ready("compatible", hash, Instant.now(), List.of()));
        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(store)
                .automaticPolling(false)
                .definition(new WorkflowDefinition("definition", 1, source))
                .definitionPreflight(DefinitionPreflightMode.FAIL)
                .build()) {
            assertTrue(engine.administration().verifyDefinitions().compatible());
        }
    }

    @Test
    void currentEnvelopeCompatibilityFixtureDecodes() throws Exception {
        Path fixture = Path.of("src/test/resources/compatibility/0.5.0/workflow-record.snapshot");
        byte[] bytes = Files.readAllBytes(fixture);
        JavaObjectCodec codec = new JavaObjectCodec();
        assertTrue(codec.inspect(bytes).enveloped());
        WorkflowRecord record = codec.decode(
                bytes, WorkflowRecord.class, getClass().getClassLoader());
        assertEquals("compatibility-fixture-0.5", record.id());
        assertEquals(WorkflowStatus.COMPLETED, record.status());
        assertEquals("fixture-output-0.5", record.output());
    }

    @Test
    void currentReleaseCompatibilityFixtureDecodes() throws Exception {
        Path fixture = Path.of("src/test/resources/compatibility/0.6.0/workflow-record.snapshot");
        byte[] bytes = Files.readAllBytes(fixture);
        JavaObjectCodec codec = new JavaObjectCodec();
        assertTrue(codec.inspect(bytes).enveloped());
        WorkflowRecord record = codec.decode(
                bytes, WorkflowRecord.class, getClass().getClassLoader());
        assertEquals("compatibility-fixture-0.6", record.id());
        assertEquals(WorkflowStatus.COMPLETED, record.status());
        assertEquals("fixture-output-0.6", record.output());
    }

    @Test
    void checkedInCompatibilityFixtureDecodes() throws Exception {
        Path fixture = Path.of("src/test/resources/compatibility/0.3.0/workflow-record.snapshot");
        byte[] bytes = Files.readAllBytes(fixture);
        JavaObjectCodec codec = new JavaObjectCodec();
        assertFalse(codec.inspect(bytes).enveloped(), "0.3.0 fixture is the legacy raw format");
        WorkflowRecord record = codec.decode(
                bytes, WorkflowRecord.class, getClass().getClassLoader());
        assertEquals("compatibility-fixture", record.id());
        assertEquals(WorkflowStatus.COMPLETED, record.status());
        assertEquals("fixture-output", record.output());
    }

    private static WorkflowRecord ready(
            String id, String hash, Instant now, List<HistoryEntry> history) {
        return new WorkflowRecord(
                id,
                0L,
                "definition",
                1,
                hash,
                WorkflowStatus.READY,
                new byte[] {1, 2, 3},
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
                history);
    }

}
