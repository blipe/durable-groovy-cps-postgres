package io.github.durablecps.store;

import io.github.durablecps.api.ConcurrentWorkflowUpdateException;
import io.github.durablecps.runtime.JavaObjectCodec;
import io.github.durablecps.runtime.ObjectCodec;
import io.github.durablecps.runtime.state.WorkflowRecord;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Local durable store: one atomically replaced record per workflow plus an OS file lock.
 * Suitable for an embedded single-node engine, including process restart recovery.
 */
public final class FileWorkflowStore implements WorkflowStore {
    private final Path root;
    private final ObjectCodec codec;

    public FileWorkflowStore(Path root) {
        this(root, new JavaObjectCodec());
    }

    public FileWorkflowStore(Path root, ObjectCodec codec) {
        try {
            this.root = root.toAbsolutePath().normalize();
            this.codec = codec;
            Files.createDirectories(this.root);
        } catch (IOException e) {
            throw new IllegalStateException("cannot initialize workflow store at " + root, e);
        }
    }

    @Override
    public WorkflowRecord create(WorkflowRecord initial) {
        validateId(initial.id());
        return locked(initial.id(), () -> {
            Path state = statePath(initial.id());
            if (Files.exists(state)) throw new IllegalStateException("workflow already exists: " + initial.id());
            writeAtomically(state, codec.encode(initial));
            return initial;
        });
    }

    @Override
    public Optional<WorkflowRecord> load(String workflowId) {
        validateId(workflowId);
        Path state = statePath(workflowId);
        if (!Files.exists(state)) return Optional.empty();
        return Optional.of(read(state));
    }

    @Override
    public WorkflowRecord replace(String workflowId, long expectedRevision, WorkflowRecord replacement) {
        validateId(workflowId);
        if (!workflowId.equals(replacement.id())) throw new IllegalArgumentException("replacement id does not match key");
        if (replacement.revision() != expectedRevision + 1) {
            throw new IllegalArgumentException("replacement revision must be expectedRevision + 1");
        }
        return locked(workflowId, () -> {
            Path state = statePath(workflowId);
            if (!Files.exists(state)) throw new IllegalStateException("workflow does not exist: " + workflowId);
            WorkflowRecord current = read(state);
            if (current.revision() != expectedRevision) {
                throw new ConcurrentWorkflowUpdateException(workflowId, expectedRevision, current.revision());
            }
            writeAtomically(state, codec.encode(replacement));
            return replacement;
        });
    }

    @Override
    public Collection<WorkflowRecord> list() {
        try (Stream<Path> paths = Files.list(root)) {
            ArrayList<WorkflowRecord> records = new ArrayList<>();
            paths.filter(path -> path.getFileName().toString().endsWith(".workflow"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .forEach(path -> records.add(read(path)));
            return records;
        } catch (IOException e) {
            throw new IllegalStateException("cannot list workflows in " + root, e);
        }
    }

    private WorkflowRecord read(Path state) {
        try {
            return codec.decode(Files.readAllBytes(state), WorkflowRecord.class, WorkflowRecord.class.getClassLoader());
        } catch (IOException e) {
            throw new IllegalStateException("cannot read workflow state " + state, e);
        }
    }

    private void writeAtomically(Path target, byte[] bytes) {
        Path temporary = root.resolve(target.getFileName() + ".tmp-" + UUID.randomUUID());
        try {
            Files.write(temporary, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
            forceDirectory(root);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw new IllegalStateException("cannot atomically write workflow state " + target, e);
        }
    }

    private static void forceDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Some file systems/providers do not allow opening directories. The state file itself was fsynced.
        }
    }

    private <T> T locked(String workflowId, IoSupplier<T> action) {
        Path lockPath = root.resolve(workflowId + ".lock");
        try (FileChannel channel = FileChannel.open(
                        lockPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            return action.get();
        } catch (IOException e) {
            throw new IllegalStateException("cannot lock workflow " + workflowId, e);
        }
    }

    private Path statePath(String workflowId) {
        return root.resolve(workflowId + ".workflow");
    }

    private static void validateId(String id) {
        if (id == null || !id.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("workflow id must match [A-Za-z0-9_.-]+");
        }
    }

    @FunctionalInterface
    private interface IoSupplier<T> {
        T get() throws IOException;
    }
}
