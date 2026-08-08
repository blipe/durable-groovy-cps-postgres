package io.github.durablecps.runtime;

import java.util.Objects;
import javax.crypto.SecretKey;

/** Named AES key. The identifier is persisted; the key material is never persisted by the engine. */
public record EncryptionKey(String id, SecretKey secretKey) {
    public EncryptionKey {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(secretKey, "secretKey");
        if (id.isBlank() || id.length() > 200) {
            throw new IllegalArgumentException("key id must be 1..200 non-blank characters");
        }
        if (!"AES".equalsIgnoreCase(secretKey.getAlgorithm())) {
            throw new IllegalArgumentException("secretKey must use AES");
        }
        byte[] encoded = secretKey.getEncoded();
        if (encoded == null || (encoded.length != 16 && encoded.length != 24 && encoded.length != 32)) {
            throw new IllegalArgumentException("AES key must be 128, 192, or 256 bits");
        }
    }
}
