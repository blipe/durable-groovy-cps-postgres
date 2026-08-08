package io.github.durablecps.admin;

/** How engine construction handles a versioned JDBC schema. */
public enum SchemaPreflightMode {
    /** Do not inspect or mutate the schema. */
    OFF,
    /** Report an incompatible schema through engine observability but continue construction. */
    WARN,
    /** Refuse construction unless the schema is at the runtime target version. */
    FAIL,
    /** Apply pending bundled migrations before definition preflight and polling. */
    MIGRATE
}
