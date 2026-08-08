package io.github.durablecps.observability;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LoggingWorkflowListener implements WorkflowListener {
    private final Logger logger;

    public LoggingWorkflowListener(Logger logger) {
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    public static LoggingWorkflowListener create() {
        return new LoggingWorkflowListener(Logger.getLogger("io.github.durablecps"));
    }

    @Override
    public void onWorkflow(WorkflowEvent event) {
        Level level = event.type() == WorkflowEventType.FAILED
                        || event.type() == WorkflowEventType.DEAD_LETTERED
                ? Level.WARNING
                : Level.INFO;
        logger.log(level, () -> "workflow event=" + event.type()
                + " id=" + event.workflowId()
                + " definition=" + event.definitionId() + "@" + event.definitionVersion()
                + " status=" + event.status()
                + " revision=" + event.revision()
                + " detail=" + event.detail());
    }

    @Override
    public void onStep(StepEvent event) {
        Level level = event.type() == StepEventType.FAILED ? Level.WARNING : Level.INFO;
        logger.log(level, () -> "step event=" + event.type()
                + " workflow=" + event.workflowId()
                + " invocation=" + event.invocationId()
                + " step=" + event.stepName()
                + " attempt=" + event.attempt() + "/" + event.maximumAttempts()
                + (event.errorType().isEmpty()
                        ? ""
                        : " error=" + event.errorType() + ":" + event.errorMessage()));
    }

    @Override
    public void onEngine(EngineEvent event) {
        logger.log(event.failure() == null ? Level.WARNING : Level.SEVERE, event.message(), event.failure());
    }
}
