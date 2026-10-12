package com.cajunsystems.gumbo.persistence;

import java.util.UUID;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * How the FoundationDB tests find a cluster, and what they do when there is none.
 *
 * <p>Without a cluster they <em>skip</em>, so a plain {@code mvn verify} stays green on a
 * machine without FoundationDB. That is the right default for a developer and the wrong one
 * for CI: a job that quietly skipped every FDB test reads exactly like one that ran them.
 * So CI sets {@value #REQUIRE_ENV}{@code =true}, and then an unreachable cluster is a
 * <strong>failure</strong> — the test suite refuses to look green on a configuration it did
 * not measure. The same rule the capability declarations follow, one level up.
 *
 * <p>The cluster file comes from {@code FDB_CLUSTER_FILE}, or the platform default when that
 * is unset. {@code scripts/fdb-tests.sh} starts a cluster in Docker and sets both variables.
 */
public final class FdbTestSupport {

    /** Set to {@code true} to turn "no cluster" from a skip into a failure. */
    public static final String REQUIRE_ENV = "GUMBO_REQUIRE_FDB";

    private FdbTestSupport() {}

    /** The cluster file path from the environment, or {@code null} for the FDB default. */
    public static String clusterFile() {
        return System.getenv("FDB_CLUSTER_FILE");
    }

    public static boolean required() {
        return "true".equalsIgnoreCase(System.getenv(REQUIRE_ENV));
    }

    /** A root subspace name nothing else is using. */
    public static String freshRoot(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * Opens and closes an adapter on a throwaway root. {@code true} if that worked.
     * {@code false} if it did not and a cluster is optional; an {@link AssertionError} if it
     * did not and {@link #REQUIRE_ENV} says one must be there.
     */
    public static boolean probe(String prefix) {
        try {
            FoundationDBPersistenceAdapter probe =
                    new FoundationDBPersistenceAdapter(clusterFile(), freshRoot(prefix + "-probe"));
            probe.open();
            probe.close();
            return true;
        } catch (Throwable e) {
            // Missing libfdb_c, no cluster file, cluster unreachable — all land here.
            if (required()) {
                throw new AssertionError(REQUIRE_ENV + "=true but no FoundationDB cluster is reachable"
                        + " (FDB_CLUSTER_FILE=" + clusterFile() + "); refusing to skip", e);
            }
            return false;
        }
    }

    /** The per-test skip, phrased once. */
    public static void assumeAvailable(boolean available) {
        assumeTrue(available, "FoundationDB cluster not available — skipping test");
    }
}
