package io.github.durablecps.store;

import io.github.durablecps.admin.StuckWorkReport;
import java.time.Duration;

/** Store-side overdue-work inspection that does not deserialize workflow snapshots. */
public interface StuckWorkStore {
    StuckWorkReport scanStuckWork(Duration overdueBy, int limit);
}
