package com.cajunsystems.gumbo.sequencer;

import com.cajunsystems.gumbo.core.OperationToken;
import com.cajunsystems.gumbo.persistence.FdbTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link FoundationDBSequencer} against a cluster. Two processes drawing distinct seqnums is
 * measured in {@code FoundationDBMultiWriterTest}; this is the single-instance contract and
 * the retry case that test cannot provoke.
 */
class FoundationDBSequencerTest {

    private static boolean fdbAvailable;

    private FoundationDBSequencer sequencer;
    private String root;

    @BeforeAll
    static void checkFdbAvailability() {
        fdbAvailable = FdbTestSupport.probe("gumbo-seq");
    }

    @BeforeEach
    void setUp() throws IOException {
        FdbTestSupport.assumeAvailable(fdbAvailable);
        root = FdbTestSupport.freshRoot("gumbo-seq");
        sequencer = new FoundationDBSequencer(FdbTestSupport.clusterFile(), root);
        sequencer.open();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (sequencer != null) sequencer.close();
    }

    @Test
    void issuesConsecutiveSeqnumsAndReportsTheGlobalTip() throws IOException {
        assertThat(sequencer.currentGlobal()).isEqualTo(-1L);
        assertThat(sequencer.next()).isEqualTo(0L);
        assertThat(sequencer.next()).isEqualTo(1L);
        assertThat(sequencer.nextBatch(3)).containsExactly(2L, 3L, 4L);
        assertThat(sequencer.current()).isEqualTo(4L);
        assertThat(sequencer.currentGlobal()).isEqualTo(4L);

        FoundationDBSequencer other = new FoundationDBSequencer(FdbTestSupport.clusterFile(), root);
        other.open();
        try {
            assertThat(other.next()).as("a second instance continues the same sequence").isEqualTo(5L);
            assertThat(sequencer.currentGlobal()).isEqualTo(5L);
        } finally {
            other.close();
        }
    }

    /**
     * What {@code db.run} does after {@code commit_unknown_result}: the same claim again, the
     * first possibly committed. Run twice, a plain read-modify-write would hand out a second
     * range and leave the first unused — a gap in the global sequence. The token recorded with
     * the claim turns the retry into a lookup of what it already holds.
     */
    @Test
    void aRetriedClaimReturnsTheSameNumbersAndAdvancesTheCounterOnce() throws IOException {
        byte[] one = OperationToken.fresh();
        List<Long> oneAttempts = new ArrayList<>();
        assertThat(sequencer.claim(1, one, oneAttempts)).isEqualTo(0L);
        assertThat(sequencer.claim(1, one, oneAttempts)).as("the retry is handed the seqnum it already claimed").isEqualTo(0L);
        assertThat(sequencer.currentGlobal()).as("the counter moved once").isEqualTo(0L);

        byte[] batch = OperationToken.fresh();
        List<Long> batchAttempts = new ArrayList<>();
        assertThat(sequencer.claim(3, batch, batchAttempts)).isEqualTo(1L);
        assertThat(sequencer.claim(3, batch, batchAttempts)).isEqualTo(1L);
        assertThat(sequencer.currentGlobal()).isEqualTo(3L);

        // Another process claims in between the lost reply and the retry: a single
        // last-claim slot would now be theirs, and the retry would claim again.
        byte[] mine = OperationToken.fresh();
        List<Long> mineAttempts = new ArrayList<>();
        assertThat(sequencer.claim(1, mine, mineAttempts)).isEqualTo(4L);
        FoundationDBSequencer other = new FoundationDBSequencer(FdbTestSupport.clusterFile(), root);
        other.open();
        try {
            assertThat(other.next()).isEqualTo(5L);
        } finally {
            other.close();
        }
        assertThat(sequencer.claim(1, mine, mineAttempts)).as("still the base this call was handed").isEqualTo(4L);
        assertThat(sequencer.currentGlobal()).as("two claims, two seqnums").isEqualTo(5L);

        assertThat(sequencer.next()).as("a fresh claim continues").isEqualTo(6L);
    }

    /** Claim records are bounded by a horizon, cleared on every claim with one range clear. */
    @Test
    void claimRecordsOlderThanTheHorizonAreClearedByTheNextClaim() {
        for (int i = 0; i < 3; i++) sequencer.next();
        assertThat(sequencer.storedClaimRecords()).isEqualTo(3L);

        sequencer.claimHorizonVersions = 0;
        sequencer.next();
        assertThat(sequencer.storedClaimRecords()).as("only the record this claim made").isEqualTo(1L);
    }
}
