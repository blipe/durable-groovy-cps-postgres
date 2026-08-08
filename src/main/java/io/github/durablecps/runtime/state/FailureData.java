package io.github.durablecps.runtime.state;

import java.io.PrintWriter;
import java.io.Serial;
import java.io.Serializable;
import java.io.StringWriter;

public record FailureData(String type, String message, String stackTrace) implements Serializable {
    @Serial private static final long serialVersionUID = 1L;

    public static FailureData from(Throwable failure) {
        StringWriter buffer = new StringWriter();
        failure.printStackTrace(new PrintWriter(buffer));
        return new FailureData(
                failure.getClass().getName(),
                failure.getMessage(),
                buffer.toString());
    }
}
