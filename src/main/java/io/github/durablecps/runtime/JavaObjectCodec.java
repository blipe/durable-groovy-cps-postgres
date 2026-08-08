package io.github.durablecps.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Java serialization codec required by the CPS object graph.
 *
 * <p>New writes use a versioned, SHA-256-protected envelope. Legacy raw Java-serialization streams
 * remain readable to permit rolling upgrades. Decode only snapshots from a trusted durable store;
 * class admission and resource limits reduce accidental exposure but do not turn Java serialization
 * into an untrusted network format.</p>
 */
public final class JavaObjectCodec implements SnapshotCodec {
    private static final int MAGIC = 0x44435053; // DCPS
    private static final int ENVELOPE_VERSION = 1;
    private static final String CODEC_ID = "java-serialization";
    private static final int CODEC_VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;

    private final DeserializationPolicy policy;

    public JavaObjectCodec() {
        this(DeserializationPolicy.productionDefaults());
    }

    public JavaObjectCodec(int maximumBytes) {
        this(copyDefaultsWithMaximumBytes(maximumBytes));
    }

    public JavaObjectCodec(DeserializationPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    private static DeserializationPolicy copyDefaultsWithMaximumBytes(int maximumBytes) {
        if (maximumBytes < 1024) throw new IllegalArgumentException("maximumBytes must be >= 1024");
        return DeserializationPolicy.builder()
                .maximumBytes(maximumBytes)
                .allowPackage("java.")
                .allowPackage("javax.")
                .allowPackage("jdk.")
                .allowPackage("sun.util.")
                .allowPackage("groovy.")
                .allowPackage("org.codehaus.groovy.")
                .allowPackage("org.kohsuke.groovy.sandbox.")
                .allowPackage("com.cloudbees.groovy.cps.")
                .allowPackage("com.google.common.")
                .allowPackage("io.github.durablecps.")
                .allowGeneratedWorkflowClasses(true)
                .build();
    }

    @Override
    public String codecId() {
        return CODEC_ID;
    }

    @Override
    public int codecVersion() {
        return CODEC_VERSION;
    }

    @Override
    public byte[] encode(Serializable value) {
        Objects.requireNonNull(value, "value");
        byte[] payload = serialize(value);
        byte[] checksum = sha256(payload);
        try {
            byte[] codec = CODEC_ID.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(payload.length + 64);
            try (DataOutputStream out = new DataOutputStream(buffer)) {
                out.writeInt(MAGIC);
                out.writeShort(ENVELOPE_VERSION);
                out.writeShort(codec.length);
                out.write(codec);
                out.writeInt(CODEC_VERSION);
                out.writeInt(payload.length);
                out.write(checksum);
                out.write(payload);
            }
            return buffer.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError("in-memory snapshot encoding failed", impossible);
        }
    }

    private byte[] serialize(Serializable value) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
                out.writeObject(value);
            }
            byte[] bytes = buffer.toByteArray();
            if (bytes.length > policy.maximumBytes()) {
                throw new IllegalStateException("serialized value exceeds " + policy.maximumBytes() + " bytes");
            }
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("cannot serialize durable state", e);
        }
    }

    @Override
    public SnapshotMetadata inspect(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        ParsedSnapshot parsed = parse(bytes, false);
        if (!parsed.enveloped()) {
            return new SnapshotMetadata(false, 0, "legacy-java-serialization", 0, bytes.length, "");
        }
        return new SnapshotMetadata(
                true,
                parsed.envelopeVersion(),
                parsed.codecId(),
                parsed.codecVersion(),
                parsed.payload().length,
                HexFormat.of().formatHex(parsed.checksum()));
    }

    @Override
    public <T> T decode(byte[] bytes, Class<T> expectedType, ClassLoader classLoader) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(expectedType, "expectedType");
        Objects.requireNonNull(classLoader, "classLoader");
        ParsedSnapshot parsed = parse(bytes, true);
        byte[] payload = parsed.payload();
        if (payload.length > policy.maximumBytes()) {
            throw new IllegalStateException("serialized value exceeds " + policy.maximumBytes() + " bytes");
        }
        try (LoaderObjectInputStream in = new LoaderObjectInputStream(
                new ByteArrayInputStream(payload), classLoader, policy)) {
            in.setObjectInputFilter(info -> {
                if (info.depth() > policy.maximumDepth()
                        || info.references() > policy.maximumReferences()
                        || info.streamBytes() > policy.maximumBytes()
                        || (info.arrayLength() >= 0 && info.arrayLength() > policy.maximumArrayLength())) {
                    return ObjectInputFilter.Status.REJECTED;
                }
                Class<?> type = info.serialClass();
                if (type != null && !policy.allowsClassName(type.getName())) {
                    return ObjectInputFilter.Status.REJECTED;
                }
                return ObjectInputFilter.Status.UNDECIDED;
            });
            Object value = in.readObject();
            return expectedType.cast(value);
        } catch (IOException | ClassNotFoundException | ClassCastException e) {
            throw new IllegalStateException("cannot deserialize durable state as " + expectedType.getName(), e);
        }
    }

    private ParsedSnapshot parse(byte[] bytes, boolean verifyChecksum) {
        if (bytes.length < 4 || readInt(bytes, 0) != MAGIC) {
            if (bytes.length > policy.maximumBytes()) {
                throw new IllegalStateException("serialized value exceeds " + policy.maximumBytes() + " bytes");
            }
            return ParsedSnapshot.legacy(bytes);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != MAGIC) throw new SnapshotIntegrityException("invalid snapshot magic");
            int envelopeVersion = in.readUnsignedShort();
            if (envelopeVersion != ENVELOPE_VERSION) {
                throw new SnapshotIntegrityException("unsupported snapshot envelope version: " + envelopeVersion);
            }
            int codecLength = in.readUnsignedShort();
            if (codecLength < 1 || codecLength > 256) throw new SnapshotIntegrityException("invalid codec id length");
            byte[] codecBytes = in.readNBytes(codecLength);
            if (codecBytes.length != codecLength) throw new EOFException("truncated codec id");
            String codecId = new String(codecBytes, StandardCharsets.UTF_8);
            int codecVersion = in.readInt();
            if (!CODEC_ID.equals(codecId)) throw new SnapshotIntegrityException("unsupported snapshot codec: " + codecId);
            if (codecVersion > CODEC_VERSION || codecVersion < 1) {
                throw new SnapshotIntegrityException("unsupported " + codecId + " version: " + codecVersion);
            }
            int payloadLength = in.readInt();
            if (payloadLength < 0 || payloadLength > policy.maximumBytes()) {
                throw new SnapshotIntegrityException("invalid snapshot payload length: " + payloadLength);
            }
            byte[] checksum = in.readNBytes(CHECKSUM_BYTES);
            if (checksum.length != CHECKSUM_BYTES) throw new EOFException("truncated snapshot checksum");
            byte[] payload = in.readNBytes(payloadLength);
            if (payload.length != payloadLength) throw new EOFException("truncated snapshot payload");
            if (in.read() != -1) throw new SnapshotIntegrityException("snapshot contains trailing bytes");
            if (verifyChecksum && !MessageDigest.isEqual(checksum, sha256(payload))) {
                throw new SnapshotIntegrityException("snapshot SHA-256 checksum mismatch");
            }
            return new ParsedSnapshot(true, envelopeVersion, codecId, codecVersion, checksum, payload);
        } catch (SnapshotIntegrityException e) {
            throw e;
        } catch (IOException e) {
            throw new SnapshotIntegrityException("malformed snapshot envelope", e);
        }
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 unavailable", e);
        }
    }

    private record ParsedSnapshot(
            boolean enveloped,
            int envelopeVersion,
            String codecId,
            int codecVersion,
            byte[] checksum,
            byte[] payload) {
        private static ParsedSnapshot legacy(byte[] payload) {
            return new ParsedSnapshot(false, 0, "legacy-java-serialization", 0, new byte[0], payload);
        }
    }

    private static final class LoaderObjectInputStream extends ObjectInputStream {
        private final ClassLoader loader;
        private final DeserializationPolicy policy;

        private LoaderObjectInputStream(InputStream input, ClassLoader loader, DeserializationPolicy policy)
                throws IOException {
            super(input);
            this.loader = loader;
            this.policy = policy;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor) throws IOException, ClassNotFoundException {
            String name = descriptor.getName();
            if (!policy.allowsClassName(name)) throw rejected(name);
            try {
                return Class.forName(name, false, loader);
            } catch (ClassNotFoundException ignored) {
                return super.resolveClass(descriptor);
            }
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
            if (!policy.allowDynamicProxies()) throw rejected("dynamic proxy");
            Class<?>[] classes = new Class<?>[interfaces.length];
            for (int i = 0; i < interfaces.length; i++) {
                if (!policy.allowsClassName(interfaces[i])) throw rejected(interfaces[i]);
                classes[i] = Class.forName(interfaces[i], false, loader);
            }
            try {
                return Proxy.getProxyClass(loader, classes);
            } catch (IllegalArgumentException e) {
                return super.resolveProxyClass(interfaces);
            }
        }

        private static InvalidClassException rejected(String name) {
            return new InvalidClassException(name, "durable snapshot class is not admitted");
        }
    }
}
