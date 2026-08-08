package io.github.durablecps.store;

import io.github.durablecps.admin.ArchiveRequest;
import io.github.durablecps.admin.ArchiveResult;
import io.github.durablecps.admin.ArchivedWorkflow;
import io.github.durablecps.admin.ArchivedWorkflowSummary;
import java.util.List;
import java.util.Optional;

public interface ArchiveCapableWorkflowStore {
    ArchiveResult archive(ArchiveRequest request);

    Optional<ArchivedWorkflow> loadArchived(String archiveId);

    List<ArchivedWorkflowSummary> listArchived(int limit, int offset);
}
