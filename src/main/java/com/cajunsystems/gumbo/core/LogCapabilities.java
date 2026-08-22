package com.cajunsystems.gumbo.core;

/**
 * What a particular log can actually do — declared per adapter, not per Gumbo.
 *
 * <p>Every capability here is optional at some layer, and until now the only way to know
 * whether a given log had one was to read documentation. That is precisely how the worst
 * defect in this codebase's history happened: a client used a seqnum-keyed read as if it
 * were version-keyed, because nothing said otherwise, and the resulting fold silently
 * double-counted. A guarantee a caller cannot interrogate is a guarantee a caller will
 * eventually assume.
 *
 * <h2>Per adapter, not per Gumbo</h2>
 * <p>The same call has different reach on different storage. {@code append(request,
 * expectedVersion)} is arbitrated across processes on FoundationDB, where the comparison
 * and the increment are one transaction; on the file adapter it is arbitrated within one
 * writer, which is sufficient there only because that adapter takes an exclusive directory
 * lock and refuses a second one. Both implement the method. They do not offer the same
 * promise, and a client that needs the stronger one has to be able to ask.
 *
 * <p>That distinction is carried by the pair {@link #conditionalAppend()} and
 * {@link #multiWriter()} rather than by a third "scope" concept: the first says the fence
 * exists and is atomic, the second says it holds against writers in other processes.
 * A runtime that distributes execution requires both, and can refuse to start against a
 * log that reports only the first — rather than discovering the difference as corruption.
 *
 * <h2>Under-reporting is the safe direction</h2>
 * <p>A capability declared {@code false} that in fact works costs functionality: a client
 * declines to use something it could have. A capability declared {@code true} that does
 * not hold costs correctness, silently. So defaults throughout this codebase are
 * conservative, and every {@code true} here is expected to be backed by a test that
 * exercises the capability rather than by prose asserting it.
 *
 * @param conditionalAppend    {@link com.cajunsystems.gumbo.persistence.PersistenceAdapter#append(PendingAppend, long)}
 *                             compares and increments atomically, rejecting a stale writer
 *                             with {@link VersionConflictException} rather than accepting it
 * @param compareAndSet        the tag key-value store arbitrates conditional mutation —
 *                             exactly one of N contending claimants is told it won
 * @param versionedReads       reads can be keyed on a tag's own {@code streamVersion}, not
 *                             only on the log's global {@code seqnum}
 * @param pushSubscriptions    new entries are delivered to subscribers rather than polled
 *                             for. Answered by the service, which implements delivery above
 *                             storage; an adapter never sets it
 * @param atomicMultiTagAppend one append can carry an entry into several tags with no
 *                             window in which some see it and others do not. Note the
 *                             entry still carries a single {@code streamVersion}, from its
 *                             primary tag, so a secondary tag's numbering is not its own —
 *                             see {@code readFromVersion}
 * @param multiWriter          several processes may write one log concurrently and have
 *                             their versions assigned consistently. When {@code false} the
 *                             log is single-writer, and a second writer is refused rather
 *                             than silently accepted
 */
public record LogCapabilities(
        boolean conditionalAppend,
        boolean compareAndSet,
        boolean versionedReads,
        boolean pushSubscriptions,
        boolean atomicMultiTagAppend,
        boolean multiWriter) {

    /** Nothing declared — what a conservative default answers. */
    public static final LogCapabilities NONE = builder().build();

    /** A builder with every capability off, to be turned on explicitly. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder seeded from {@code from}, for composing an answer over another log's —
     * a decorator that narrows what its delegate offers, most of all.
     */
    public static Builder builder(LogCapabilities from) {
        return new Builder()
                .conditionalAppend(from.conditionalAppend())
                .compareAndSet(from.compareAndSet())
                .versionedReads(from.versionedReads())
                .pushSubscriptions(from.pushSubscriptions())
                .atomicMultiTagAppend(from.atomicMultiTagAppend())
                .multiWriter(from.multiWriter());
    }

    /**
     * Builds a {@link LogCapabilities}.
     *
     * <p>Six positional booleans is exactly the constructor call where two of them get
     * transposed and nothing complains, so the record is built by name.
     */
    public static final class Builder {
        private boolean conditionalAppend;
        private boolean compareAndSet;
        private boolean versionedReads;
        private boolean pushSubscriptions;
        private boolean atomicMultiTagAppend;
        private boolean multiWriter;

        private Builder() {}

        public Builder conditionalAppend(boolean value) {
            this.conditionalAppend = value;
            return this;
        }

        public Builder compareAndSet(boolean value) {
            this.compareAndSet = value;
            return this;
        }

        public Builder versionedReads(boolean value) {
            this.versionedReads = value;
            return this;
        }

        public Builder pushSubscriptions(boolean value) {
            this.pushSubscriptions = value;
            return this;
        }

        public Builder atomicMultiTagAppend(boolean value) {
            this.atomicMultiTagAppend = value;
            return this;
        }

        public Builder multiWriter(boolean value) {
            this.multiWriter = value;
            return this;
        }

        public LogCapabilities build() {
            return new LogCapabilities(conditionalAppend, compareAndSet, versionedReads,
                    pushSubscriptions, atomicMultiTagAppend, multiWriter);
        }
    }
}
