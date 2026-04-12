package com.cajunsystems.gumbo.core;

/**
 * A cursor into the shared log expressed as a global sequence number.
 *
 * <p>{@code LogPosition.BEGINNING} starts before the first entry ever written.
 * {@code LogPosition.END} is always past the latest entry (useful for "tail" reads).
 *
 * <p>Positions are comparable: a position is "before" another if its seqnum is
 * strictly smaller.
 */
public record LogPosition(long seqnum) implements Comparable<LogPosition> {

    /** Position before the very first entry (seqnum 0). */
    public static final LogPosition BEGINNING = new LogPosition(0L);

    /** Sentinel meaning "after the latest entry". Use for tail subscriptions. */
    public static final LogPosition END = new LogPosition(Long.MAX_VALUE);

    public LogPosition {
        if (seqnum < 0) throw new IllegalArgumentException("seqnum must be >= 0, was: " + seqnum);
    }

    /** Returns the position immediately after this one. */
    public LogPosition next() {
        if (seqnum == Long.MAX_VALUE) return this;
        return new LogPosition(seqnum + 1);
    }

    public boolean isBefore(LogPosition other) {
        return this.seqnum < other.seqnum;
    }

    public boolean isAfterOrEqualTo(LogPosition other) {
        return this.seqnum >= other.seqnum;
    }

    @Override
    public int compareTo(LogPosition other) {
        return Long.compare(this.seqnum, other.seqnum);
    }

    @Override
    public String toString() {
        if (seqnum == Long.MAX_VALUE) return "LogPosition{END}";
        return "LogPosition{seqnum=" + seqnum + '}';
    }
}
