package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.core.LogCapabilities;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.sequencer.LocalSequencer;
import com.cajunsystems.gumbo.sequencer.Sequencer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the whole log can do is not what its storage can do.
 *
 * <p>{@code multiWriter} needs two independent things — storage that assigns per-tag versions across
 * processes, and a sequencer whose global {@code seqnum} spans them — and an adapter answers for
 * only the first. The service owns the second, and its default is a per-process {@code AtomicLong}.
 *
 * <p>The configuration these tests exist for is a FoundationDB adapter behind the default
 * sequencer: every stream version arbitrated correctly by storage, and two processes still handing
 * out the same seqnums. Nothing downstream catches it. The seqnum never passes through the
 * adapter's fence, and both durable adapters index <em>by</em> seqnum, so a collision overwrites an
 * index entry and the earlier record stops being readable while its bytes stay on disk. A
 * distributed caller that checked the flag before starting would have been told yes.
 */
class LogCapabilityCompositionTest {

    private SharedLogService service;

    @AfterEach
    void tearDown() {
        if (service != null) service.close();
    }

    /** A storage layer that arbitrates everything, to isolate the sequencer's contribution. */
    private static final class FullyCapableAdapter extends InMemoryPersistenceAdapter {
        @Override public LogCapabilities capabilities() {
            return LogCapabilities.builder(super.capabilities()).multiWriter(true).build();
        }
    }

    /** Stands in for {@code FoundationDBSequencer}, which needs a live cluster. */
    private static final class DistributedSequencer implements Sequencer {
        private final AtomicLong counter = new AtomicLong();
        @Override public long next() { return counter.getAndIncrement(); }
        @Override public long current() { return counter.get() - 1; }
        @Override public boolean distributed() { return true; }
    }

    @Test
    void aMultiWriterAdapterBehindTheDefaultSequencerIsNotAMultiWriterLog() throws IOException {
        service = open(new LocalSequencer());

        assertThat(service.capabilities().multiWriter())
                .as("storage arbitrates versions, but seqnums would still collide across processes")
                .isFalse();
        assertThat(service.capabilities().conditionalAppend())
                .as("the storage-side fence is untouched — only the whole-log claim narrows")
                .isTrue();
    }

    @Test
    void bothHalvesPresentReportsAMultiWriterLog() throws IOException {
        service = open(new DistributedSequencer());

        assertThat(service.capabilities().multiWriter()).isTrue();
    }

    /** A distributed sequencer cannot make up for storage that assigns versions per process. */
    @Test
    void aDistributedSequencerDoesNotRescueSingleWriterStorage() throws IOException {
        service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())   // declares multiWriter false
                .sequencer(new DistributedSequencer())
                .build());

        assertThat(service.capabilities().multiWriter()).isFalse();
    }

    /**
     * The default is conservative for the same reason every other default here is: a sequencer that
     * has not considered the question is assumed not to satisfy it. {@code LocalSequencer} has
     * considered it and the answer is genuinely no — its counter is seeded per process.
     */
    @Test
    void theDefaultSequencerDeclaresItselfProcessLocal() {
        assertThat(new LocalSequencer().distributed()).isFalse();
        assertThat(new Sequencer() {
            @Override public long next() { return 0; }
            @Override public long current() { return -1; }
        }.distributed()).as("an implementation that says nothing declares nothing").isFalse();
    }

    /** Subscriptions stay the service's own answer, unaffected by any of this. */
    @Test
    void pushSubscriptionsRemainsTheServicesOwnAnswer() throws IOException {
        service = open(new LocalSequencer());

        assertThat(service.capabilities().pushSubscriptions()).isTrue();
        assertThat(new FullyCapableAdapter().capabilities().pushSubscriptions())
                .as("an adapter never answers for delivery it does not implement")
                .isFalse();
    }

    private static SharedLogService open(Sequencer sequencer) throws IOException {
        return SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new FullyCapableAdapter())
                .sequencer(sequencer)
                .build());
    }
}
