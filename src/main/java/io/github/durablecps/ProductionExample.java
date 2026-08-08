package io.github.durablecps;

import io.github.durablecps.admin.DefinitionPreflightMode;
import io.github.durablecps.admin.RetentionPolicy;
import io.github.durablecps.admin.SchemaPreflightMode;
import io.github.durablecps.api.StepHandler;
import io.github.durablecps.api.WorkflowDefinition;
import io.github.durablecps.runtime.AesGcmObjectCodec;
import io.github.durablecps.runtime.DeserializationPolicy;
import io.github.durablecps.runtime.DurableWorkflowEngine;
import io.github.durablecps.runtime.EncryptionKeyProvider;
import io.github.durablecps.runtime.JavaObjectCodec;
import io.github.durablecps.store.JdbcWorkflowStore;
import java.time.Duration;
import javax.sql.DataSource;

/** Complete minimal-production assembly without choosing a framework or connection pool. */
public final class ProductionExample {
    private ProductionExample() {}

    public static DurableWorkflowEngine create(
            DataSource dataSource,
            EncryptionKeyProvider encryptionKeys,
            StepHandler chargeCard,
            StepHandler createShipment) {
        JavaObjectCodec serialized = new JavaObjectCodec(DeserializationPolicy.builder()
                .allowPackage("java.")
                .allowPackage("groovy.")
                .allowPackage("org.codehaus.groovy.")
                .allowPackage("org.kohsuke.groovy.sandbox.")
                .allowPackage("com.cloudbees.groovy.cps.")
                .allowPackage("com.google.common.")
                .allowPackage("io.github.durablecps.")
                .allowPackage("com.mycompany.orders.")
                .allowGeneratedWorkflowClasses(true)
                .build());
        AesGcmObjectCodec codec = new AesGcmObjectCodec(serialized, encryptionKeys);
        JdbcWorkflowStore store = new JdbcWorkflowStore(dataSource, codec);

        WorkflowDefinition order = new WorkflowDefinition("order", 1, """
                def payment = step('charge-card', [
                    orderId: input('orderId'),
                    amount: input('amount')
                ])
                def approval = awaitSignal('approve-order')
                if (approval != 'approved') return [status: 'rejected', payment: payment]
                def shipment = step('create-shipment', [orderId: input('orderId')])
                return [status: 'shipped', payment: payment, shipment: shipment]
                """);

        return DurableWorkflowEngine.builder(store)
                .codec(codec)
                .schemaPreflight(SchemaPreflightMode.MIGRATE)
                .definitionPreflight(DefinitionPreflightMode.FAIL)
                .definition(order)
                .step("charge-card", chargeCard)
                .step("create-shipment", createShipment)
                .stepConcurrencyLimit("charge-card", 32)
                .stepConcurrencyLimit("create-shipment", 64)
                .maximumInFlightSteps(256)
                .pollInterval(Duration.ofMillis(250))
                .workflowLease(Duration.ofSeconds(30))
                .stepLease(Duration.ofSeconds(30))
                .retentionPolicy(RetentionPolicy.standard(Duration.ofDays(30)))
                .shutdownTimeout(Duration.ofSeconds(20))
                .build();
    }
}
