package com.central.sharedlog.service;

import com.central.sharedlog.persistence.InMemoryPersistenceAdapter;
import com.central.sharedlog.persistence.PersistenceAdapter;
import com.central.sharedlog.sequencer.LocalSequencer;
import com.central.sharedlog.sequencer.Sequencer;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Immutable configuration for {@link SharedLogService}.
 *
 * <p>Use {@link #builder()} to construct instances:
 * <pre>{@code
 * SharedLogConfig config = SharedLogConfig.builder()
 *     .persistenceAdapter(new FileBasedPersistenceAdapter("/data/logs"))
 *     .asyncPool(Executors.newVirtualThreadPerTaskExecutor())
 *     .build();
 * }</pre>
 */
public final class SharedLogConfig {

    private final PersistenceAdapter persistenceAdapter;
    private final Sequencer sequencer;
    private final ExecutorService asyncPool;
    private final int defaultReadBatchSize;

    private SharedLogConfig(Builder b) {
        this.persistenceAdapter  = b.persistenceAdapter;
        this.sequencer           = b.sequencer;
        this.asyncPool           = b.asyncPool;
        this.defaultReadBatchSize = b.defaultReadBatchSize;
    }

    public PersistenceAdapter getPersistenceAdapter() { return persistenceAdapter; }
    public Sequencer getSequencer() { return sequencer; }
    public ExecutorService getAsyncPool() { return asyncPool; }
    public int getDefaultReadBatchSize() { return defaultReadBatchSize; }

    public static Builder builder() { return new Builder(); }

    // -------------------------------------------------------------------------

    public static final class Builder {

        private PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        private Sequencer sequencer = new LocalSequencer();
        /**
         * Defaults to a virtual-thread-per-task executor so async operations
         * (reads, subscription delivery) never block a platform thread.
         */
        private ExecutorService asyncPool = Executors.newVirtualThreadPerTaskExecutor();
        private int defaultReadBatchSize = 1024;

        /** Sets the storage back-end. */
        public Builder persistenceAdapter(PersistenceAdapter adapter) {
            this.persistenceAdapter = Objects.requireNonNull(adapter, "adapter");
            return this;
        }

        /** Overrides the sequencer (e.g. a distributed one for multi-node deployments). */
        public Builder sequencer(Sequencer sequencer) {
            this.sequencer = Objects.requireNonNull(sequencer, "sequencer");
            return this;
        }

        /**
         * Sets the thread pool used for async reads and subscription dispatch.
         * Defaults to {@link Executors#newVirtualThreadPerTaskExecutor()}.
         */
        public Builder asyncPool(ExecutorService asyncPool) {
            this.asyncPool = Objects.requireNonNull(asyncPool, "asyncPool");
            return this;
        }

        /** Maximum entries returned in a single read batch (default: 1024). */
        public Builder defaultReadBatchSize(int size) {
            if (size <= 0) throw new IllegalArgumentException("size must be > 0");
            this.defaultReadBatchSize = size;
            return this;
        }

        public SharedLogConfig build() {
            return new SharedLogConfig(this);
        }
    }
}
