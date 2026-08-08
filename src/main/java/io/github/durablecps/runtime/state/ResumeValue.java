package io.github.durablecps.runtime.state;

import java.io.Serial;
import java.io.Serializable;

public sealed interface ResumeValue extends Serializable permits ResumeValue.Success, ResumeValue.Failure {
    record Success(Serializable value) implements ResumeValue {
        @Serial private static final long serialVersionUID = 1L;
    }

    record Failure(FailureData failure) implements ResumeValue {
        @Serial private static final long serialVersionUID = 1L;

        public Failure {
            if (failure == null) throw new NullPointerException("failure");
        }
    }
}
