package com.cajunsystems.gumbo.sequencer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Sequencer#nextBatch(int)} on both {@link LocalSequencer}
 * and the default interface implementation.
 */
class SequencerBatchTest {

    // -------------------------------------------------------------------------
    // LocalSequencer — overrides nextBatch with AtomicLong.getAndAdd
    // -------------------------------------------------------------------------

    @Test
    void localSequencer_nextBatch_returnsConsecutiveSeqnums() {
        LocalSequencer seq = new LocalSequencer();
        long[] batch = seq.nextBatch(5);

        assertThat(batch).hasSize(5);
        for (int i = 0; i < batch.length; i++) {
            assertThat(batch[i]).isEqualTo(i);
        }
    }

    @Test
    void localSequencer_nextBatch_allValuesUnique() {
        LocalSequencer seq = new LocalSequencer();
        long[] batch = seq.nextBatch(64);

        Set<Long> seen = new HashSet<>();
        for (long v : batch) seen.add(v);
        assertThat(seen).hasSize(64);
    }

    @Test
    void localSequencer_nextBatch_advancesCurrentPosition() {
        LocalSequencer seq = new LocalSequencer();
        seq.nextBatch(10);

        assertThat(seq.current()).isEqualTo(9L);
    }

    @Test
    void localSequencer_nextBatch_countOfOne_matchesNext() {
        LocalSequencer seqA = new LocalSequencer(100);
        LocalSequencer seqB = new LocalSequencer(100);

        long single  = seqA.next();
        long[] batch = seqB.nextBatch(1);

        assertThat(batch[0]).isEqualTo(single);
    }

    @Test
    void localSequencer_nextBatch_subsequentCallsContinueMonotonically() {
        LocalSequencer seq = new LocalSequencer();
        long[] first  = seq.nextBatch(3);  // [0, 1, 2]
        long[] second = seq.nextBatch(3);  // [3, 4, 5]

        assertThat(second[0]).isGreaterThan(first[first.length - 1]);
        assertThat(second[0]).isEqualTo(first[first.length - 1] + 1);
    }

    @Test
    void localSequencer_nextBatch_interleavedWithNext() {
        LocalSequencer seq = new LocalSequencer();
        long s0    = seq.next();          // 0
        long[] b   = seq.nextBatch(3);    // [1, 2, 3]
        long s4    = seq.next();          // 4

        assertThat(s0).isEqualTo(0);
        assertThat(b).containsExactly(1, 2, 3);
        assertThat(s4).isEqualTo(4);
    }

    @Test
    void localSequencer_nextBatch_zeroCountThrows() {
        LocalSequencer seq = new LocalSequencer();
        assertThatThrownBy(() -> seq.nextBatch(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count must be > 0");
    }

    @Test
    void localSequencer_nextBatch_negativeCountThrows() {
        LocalSequencer seq = new LocalSequencer();
        assertThatThrownBy(() -> seq.nextBatch(-5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------------------------------
    // Default interface implementation (anonymous inner class)
    // -------------------------------------------------------------------------

    @Test
    void defaultImpl_nextBatch_delegatesToNext() {
        // Create a Sequencer that uses only the default nextBatch (no override)
        LocalSequencer delegate = new LocalSequencer();
        Sequencer defaultImpl = new Sequencer() {
            @Override public long next()    { return delegate.next(); }
            @Override public long current() { return delegate.current(); }
            // nextBatch is NOT overridden — uses default loop
        };

        long[] batch = defaultImpl.nextBatch(4);

        assertThat(batch).hasSize(4);
        // Values should be unique and monotonic even through the default loop
        for (int i = 0; i < batch.length - 1; i++) {
            assertThat(batch[i]).isLessThan(batch[i + 1]);
        }
        // All values distinct
        Set<Long> distinct = new HashSet<>();
        Arrays.stream(batch).forEach(distinct::add);
        assertThat(distinct).hasSize(4);
    }

    @Test
    void defaultImpl_nextBatch_zeroCountThrows() {
        Sequencer seq = new Sequencer() {
            @Override public long next()    { return 0; }
            @Override public long current() { return -1; }
        };
        assertThatThrownBy(() -> seq.nextBatch(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
