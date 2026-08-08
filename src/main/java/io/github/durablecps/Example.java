package io.github.durablecps;

import io.github.durablecps.api.WorkflowDefinition;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.store.FileWorkflowStore;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class Example {
    private Example() {
    }

    public static void main(String[] args) throws Exception {
        Path project = Path.of("").toAbsolutePath();
        String source = Files.readString(project.resolve("examples/order-workflow.groovy"));
        WorkflowDefinition definition = new WorkflowDefinition("order", 1, source);

        try (DurableWorkflowEngine engine = DurableWorkflowEngine.builder(
                        new FileWorkflowStore(project.resolve(".durable-cps")))
                .definition(definition)
                .step("charge-card", (request, context) -> {
                    System.out.println("charge idempotency-key=" + request.invocationId() + " " + request.argument());
                    return CompletableFuture.completedFuture((Serializable) ("payment-" + request.workflowId()));
                })
                .step("create-shipment", (request, context) -> {
                    System.out.println("ship idempotency-key=" + request.invocationId() + " " + request.argument());
                    return CompletableFuture.completedFuture((Serializable) ("shipment-" + request.workflowId()));
                })
                .pollInterval(Duration.ofMillis(25))
                .build()) {
            String id = engine.start("demo-order", "order", Map.of(
                    "orderId", "A-100",
                    "amount", 1250L));
            System.out.println("started " + id + " -> " + engine.get(id).orElseThrow());

            engine.signal(id, "approve-order", "approved");
            Thread.sleep(500L);
            System.out.println("finished -> " + engine.get(id).orElseThrow());
        }
    }
}
