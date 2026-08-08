package io.github.durablecps.runtime;

import java.io.Serializable;

public interface ObjectCodec {
    byte[] encode(Serializable value);

    <T> T decode(byte[] bytes, Class<T> expectedType, ClassLoader classLoader);
}
