package io.github.durablecps.store;

import java.util.List;

/** Optional optimized inventory that does not deserialize every workflow snapshot. */
public interface DefinitionInventoryStore {
    List<StoredDefinitionReference> definitionInventory();
}
