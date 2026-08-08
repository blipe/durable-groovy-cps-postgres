package io.github.durablecps.api;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/** The only legal suspension values produced by a durable script. */
public sealed interface DurableCommand extends Serializable
        permits DurableCommand.Step, DurableCommand.Sleep, DurableCommand.AwaitSignal {

    long sequence();

    record Step(long sequence, String name, Serializable argument, StepOptions options)
            implements DurableCommand {
        @Serial private static final long serialVersionUID = 1L;

        public Step {
            if (sequence < 1) throw new IllegalArgumentException("sequence must be >= 1");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(options, "options");
            if (name.isBlank()) throw new IllegalArgumentException("step name must not be blank");
        }
    }

    record Sleep(long sequence, Duration duration) implements DurableCommand {
        @Serial private static final long serialVersionUID = 1L;

        public Sleep {
            if (sequence < 1) throw new IllegalArgumentException("sequence must be >= 1");
            Objects.requireNonNull(duration, "duration");
            if (duration.isNegative()) throw new IllegalArgumentException("duration must not be negative");
        }
    }

    record AwaitSignal(long sequence, String name) implements DurableCommand {
        @Serial private static final long serialVersionUID = 1L;

        public AwaitSignal {
            if (sequence < 1) throw new IllegalArgumentException("sequence must be >= 1");
            Objects.requireNonNull(name, "name");
            if (name.isBlank()) throw new IllegalArgumentException("signal name must not be blank");
        }
    }
}
