package io.github.durablecps.runtime;

/** Supplies the active write key and historical read keys for rotation. */
public interface EncryptionKeyProvider {
    EncryptionKey activeKey();

    EncryptionKey require(String keyId);
}
