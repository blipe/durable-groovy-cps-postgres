package io.github.durablecps.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;

/**
 * Authenticated AES-GCM envelope around another codec.
 *
 * <p>New writes use the active key id. Reads resolve historical ids, enabling rotation. Bytes that
 * do not carry the encryption magic are delegated unchanged, allowing encryption to be enabled in
 * a rolling deployment after every reader has this codec.</p>
 */
public final class AesGcmObjectCodec implements SnapshotCodec {
    private static final int MAGIC = 0x44435045; // DCPE
    private static final int VERSION = 1;
    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final String CODEC_ID = "aes-gcm";

    private final SnapshotCodec delegate;
    private final EncryptionKeyProvider keys;
    private final SecureRandom random;
    private final int maximumEncryptedBytes;

    public AesGcmObjectCodec(SnapshotCodec delegate, EncryptionKeyProvider keys) {
        this(delegate, keys, new SecureRandom(), 64 * 1024 * 1024);
    }

    public AesGcmObjectCodec(
            SnapshotCodec delegate,
            EncryptionKeyProvider keys,
            SecureRandom random,
            int maximumEncryptedBytes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.random = Objects.requireNonNull(random, "random");
        if (maximumEncryptedBytes < 1024) {
            throw new IllegalArgumentException("maximumEncryptedBytes must be >= 1024");
        }
        this.maximumEncryptedBytes = maximumEncryptedBytes;
    }

    @Override
    public String codecId() {
        return CODEC_ID + "/" + delegate.codecId();
    }

    @Override
    public int codecVersion() {
        return VERSION;
    }

    @Override
    public byte[] encode(Serializable value) {
        byte[] plaintext = delegate.encode(value);
        EncryptionKey active = keys.activeKey();
        byte[] keyId = active.id().getBytes(StandardCharsets.UTF_8);
        if (keyId.length < 1 || keyId.length > 255) {
            throw new IllegalStateException("UTF-8 key id must be 1..255 bytes");
        }
        byte[] nonce = new byte[NONCE_BYTES];
        random.nextBytes(nonce);
        byte[] aad = aad(keyId, nonce);
        byte[] ciphertext = crypt(Cipher.ENCRYPT_MODE, active, nonce, aad, plaintext);
        if (ciphertext.length > maximumEncryptedBytes) {
            throw new IllegalStateException("encrypted snapshot exceeds " + maximumEncryptedBytes + " bytes");
        }
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(ciphertext.length + 64);
            try (DataOutputStream out = new DataOutputStream(buffer)) {
                out.writeInt(MAGIC);
                out.writeShort(VERSION);
                out.writeByte(keyId.length);
                out.write(keyId);
                out.writeByte(nonce.length);
                out.write(nonce);
                out.writeInt(ciphertext.length);
                out.write(ciphertext);
            }
            return buffer.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    @Override
    public <T> T decode(byte[] bytes, Class<T> expectedType, ClassLoader classLoader) {
        Objects.requireNonNull(bytes, "bytes");
        if (!encrypted(bytes)) return delegate.decode(bytes, expectedType, classLoader);
        Parsed parsed = parse(bytes);
        EncryptionKey key = keys.require(parsed.keyId());
        byte[] plaintext = crypt(
                Cipher.DECRYPT_MODE,
                key,
                parsed.nonce(),
                aad(parsed.keyId().getBytes(StandardCharsets.UTF_8), parsed.nonce()),
                parsed.ciphertext());
        return delegate.decode(plaintext, expectedType, classLoader);
    }

    @Override
    public SnapshotMetadata inspect(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (!encrypted(bytes)) return delegate.inspect(bytes);
        Parsed parsed = parse(bytes);
        EncryptionKey key = keys.require(parsed.keyId());
        byte[] plaintext = crypt(
                Cipher.DECRYPT_MODE,
                key,
                parsed.nonce(),
                aad(parsed.keyId().getBytes(StandardCharsets.UTF_8), parsed.nonce()),
                parsed.ciphertext());
        SnapshotMetadata inner = delegate.inspect(plaintext);
        return new SnapshotMetadata(
                true,
                VERSION,
                codecId() + "#" + parsed.keyId(),
                VERSION,
                parsed.ciphertext().length,
                inner.sha256());
    }

    private Parsed parse(byte[] bytes) {
        if (bytes.length > maximumEncryptedBytes + 512) {
            throw new SnapshotIntegrityException("encrypted snapshot exceeds configured limit");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != MAGIC) throw new SnapshotIntegrityException("invalid encryption magic");
            int version = in.readUnsignedShort();
            if (version != VERSION) throw new SnapshotIntegrityException("unsupported encryption envelope: " + version);
            int keyLength = in.readUnsignedByte();
            if (keyLength < 1) throw new SnapshotIntegrityException("missing encryption key id");
            byte[] keyId = in.readNBytes(keyLength);
            if (keyId.length != keyLength) throw new EOFException("truncated encryption key id");
            int nonceLength = in.readUnsignedByte();
            if (nonceLength != NONCE_BYTES) throw new SnapshotIntegrityException("invalid AES-GCM nonce length");
            byte[] nonce = in.readNBytes(nonceLength);
            if (nonce.length != nonceLength) throw new EOFException("truncated AES-GCM nonce");
            int length = in.readInt();
            if (length < 16 || length > maximumEncryptedBytes) {
                throw new SnapshotIntegrityException("invalid encrypted payload length: " + length);
            }
            byte[] ciphertext = in.readNBytes(length);
            if (ciphertext.length != length) throw new EOFException("truncated encrypted payload");
            if (in.read() != -1) throw new SnapshotIntegrityException("encrypted snapshot contains trailing bytes");
            return new Parsed(new String(keyId, StandardCharsets.UTF_8), nonce, ciphertext);
        } catch (SnapshotIntegrityException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new SnapshotIntegrityException("malformed encryption envelope", failure);
        }
    }

    private byte[] crypt(
            int mode,
            EncryptionKey key,
            byte[] nonce,
            byte[] aad,
            byte[] input) {
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(mode, key.secretKey(), new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(aad);
            return cipher.doFinal(input);
        } catch (GeneralSecurityException failure) {
            throw new SnapshotIntegrityException(
                    mode == Cipher.DECRYPT_MODE
                            ? "encrypted snapshot authentication failed"
                            : "cannot encrypt durable snapshot",
                    failure);
        }
    }

    private static byte[] aad(byte[] keyId, byte[] nonce) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(buffer)) {
                out.writeInt(MAGIC);
                out.writeShort(VERSION);
                out.writeByte(keyId.length);
                out.write(keyId);
                out.writeByte(nonce.length);
                out.write(nonce);
            }
            return buffer.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static boolean encrypted(byte[] bytes) {
        return bytes.length >= 4
                && ((bytes[0] & 0xff) << 24
                        | (bytes[1] & 0xff) << 16
                        | (bytes[2] & 0xff) << 8
                        | (bytes[3] & 0xff)) == MAGIC;
    }

    private record Parsed(String keyId, byte[] nonce, byte[] ciphertext) {}
}
