package com.cajunsystems.gumbo.core;

/**
 * Arithmetic on stream versions, in one place so the edge cases are handled once.
 *
 * <p>Every exclusive version-keyed read is expressed as an inclusive one, and that
 * conversion has two ends that both bite: a sentinel below the first version, and a
 * cursor at the largest value a {@code long} can hold.
 */
public final class StreamVersions {

    private StreamVersions() {}

    /**
     * Converts an exclusive lower bound ("after version N") into the inclusive bound the
     * read APIs take.
     *
     * <p>Saturates instead of wrapping. {@code Long.MAX_VALUE + 1} is
     * {@code Long.MIN_VALUE}, which every version filter reads as "no lower bound" — so
     * a cursor at the maximum would return the caller's <em>entire</em> stream when it
     * asked for the part after everything. That is not a hypothetical input here:
     * {@code Long.MAX_VALUE} is already the established sentinel for "the very end" in
     * this codebase, as {@code DefaultLogView.checkTail} uses it.
     *
     * <p>Negative bounds clamp to {@code 0}, so {@code -1} reads the whole stream.
     *
     * @param afterVersion exclusive lower bound; {@code -1} for the whole stream
     * @return the equivalent inclusive lower bound
     */
    public static long afterToInclusive(long afterVersion) {
        if (afterVersion < 0) return 0L;
        return afterVersion == Long.MAX_VALUE ? Long.MAX_VALUE : afterVersion + 1;
    }
}
