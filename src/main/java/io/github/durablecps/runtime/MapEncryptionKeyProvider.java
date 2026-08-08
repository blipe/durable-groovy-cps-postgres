package io.github.durablecps.runtime;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable key provider convenient for environment/KMS bootstrap code and tests. */
public final class MapEncryptionKeyProvider implements EncryptionKeyProvider {
    private final String activeKeyId;
    private final Map<String, EncryptionKey> keys;

    public MapEncryptionKeyProvider(String activeKeyId, Iterable<EncryptionKey> keys) {
        this.activeKeyId = Objects.requireNonNull(activeKeyId, "activeKeyId");
        LinkedHashMap<String, EncryptionKey> copy = new LinkedHashMap<>();
        for (EncryptionKey key : keys) {
            EncryptionKey previous = copy.putIfAbsent(key.id(), key);
            if (previous != null) throw new IllegalArgumentException("duplicate key id: " + key.id());
        }
        if (!copy.containsKey(activeKeyId)) throw new IllegalArgumentException("active key is missing: " + activeKeyId);
        this.keys = Map.copyOf(copy);
    }

    @Override
    public EncryptionKey activeKey() {
        return keys.get(activeKeyId);
    }

    @Override
    public EncryptionKey require(String keyId) {
        EncryptionKey key = keys.get(keyId);
        if (key == null) throw new SnapshotIntegrityException("unknown encryption key id: " + keyId);
        return key;
    }
}
