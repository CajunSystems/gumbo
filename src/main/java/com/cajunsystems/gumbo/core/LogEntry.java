package com.cajunsystems.gumbo.core;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable entry in the shared log.
 *
 * <p>Each entry carries:
 * <ul>
 *   <li>{@code seqnum}  – global, monotonically increasing sequence number assigned by
 *       the sequencer; provides total ordering across the entire log.</li>
 *   <li>a {@code streamVersion} <strong>per tag</strong> – this entry's position within
 *       that tag's stream, numbered densely from zero. Each tag counts its own entries,
 *       so an entry appended to two tags holds two positions, one in each.</li>
 *   <li>{@code tags}    – the set of logical streams this entry belongs to. An entry
 *       may be visible from multiple tag views (e.g. both {@code "orders"} and
 *       {@code "orders:order-42"}).</li>
 *   <li>{@code data}    – opaque payload bytes; interpretation is left to the caller.</li>
 *   <li>{@code timestamp} – wall-clock time at which the entry was appended.</li>
 * </ul>
 *
 * <h2>Why a version per tag</h2>
 * <p>An entry used to carry exactly one version, drawn from its primary tag, and every
 * tag it touched was told that number. A tag carried only as a <em>secondary</em> tag
 * therefore inherited another stream's numbering: its versions were not dense, did not
 * start at zero, and could go backwards relative to entries already delivered — so a
 * consumer holding a cursor into a shared fan-out tag could silently skip work. That is
 * exactly the shape a workflow engine uses when one atomic append records history and
 * enqueues the work item, which is what made it worth the format change to fix.
 *
 * <h2>Entries written before that fix</h2>
 * <p>Such an entry holds one number and <em>cannot</em> say which tag it belonged to:
 * the primary tag was {@code tags.iterator().next()} over an immutable {@code Set}, whose
 * iteration order Java salts per JVM run. Rather than guess, those entries answer every
 * tag they carry with that single number — precisely what they did before — and report
 * {@link #hasPerTagVersions()} as {@code false}. A consumer that needs to know whether a
 * position is really its own stream's can ask, instead of assuming.
 *
 * <p>Because {@code data} is a mutable byte array, this class implements defensive
 * copying in its constructor and custom {@code equals}/{@code hashCode}.
 */
public final class LogEntry {

    /** Returned by {@link #streamVersion(LogTag)} for a tag this entry does not carry. */
    public static final long UNKNOWN_VERSION = -1L;

    private final long seqnum;
    private final Map<LogTag, Long> streamVersions;
    private final boolean perTagVersions;
    private final Set<LogTag> tags;
    private final byte[] data;
    private final Instant timestamp;

    /**
     * An entry carrying one version whose tag attribution is unknown — the shape every
     * entry had before versions were tracked per tag, and what decoding an older record
     * produces.
     *
     * <p>For a single-tag entry, which is the overwhelmingly common case, this is exact:
     * one tag, one version, nothing ambiguous. It is only a multi-tag entry that cannot
     * say which of its tags the number counts.
     */
    public LogEntry(long seqnum, long streamVersion, Set<LogTag> tags, byte[] data, Instant timestamp) {
        this(seqnum, uniformVersions(tags, streamVersion), tags, data, timestamp,
                /* perTagVersions */ tags != null && tags.size() == 1);
    }

    /**
     * An entry carrying each tag's own position in its own stream.
     *
     * @param streamVersions one entry per tag in {@code tags}; must cover all of them
     */
    public LogEntry(long seqnum, Map<LogTag, Long> streamVersions, Set<LogTag> tags,
                    byte[] data, Instant timestamp) {
        this(seqnum, streamVersions, tags, data, timestamp, /* perTagVersions */ true);
    }

    private LogEntry(long seqnum, Map<LogTag, Long> streamVersions, Set<LogTag> tags,
                     byte[] data, Instant timestamp, boolean perTagVersions) {
        if (seqnum < 0) throw new IllegalArgumentException("seqnum must be >= 0");
        Objects.requireNonNull(tags, "tags");
        if (tags.isEmpty()) throw new IllegalArgumentException("tags must not be empty");
        Objects.requireNonNull(streamVersions, "streamVersions");
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(timestamp, "timestamp");

        Map<LogTag, Long> copy = new LinkedHashMap<>();
        for (LogTag tag : tags) {
            Long v = streamVersions.get(tag);
            if (v == null) {
                throw new IllegalArgumentException("no streamVersion for tag " + tag
                        + "; every tag the entry carries needs its own position");
            }
            if (v < 0) throw new IllegalArgumentException("streamVersion must be >= 0, was " + v);
            copy.put(tag, v);
        }

        this.seqnum = seqnum;
        this.streamVersions = Collections.unmodifiableMap(copy);
        this.perTagVersions = perTagVersions;
        this.tags = Collections.unmodifiableSet(Set.copyOf(tags));
        this.data = Arrays.copyOf(data, data.length);
        this.timestamp = timestamp;
    }

    private static Map<LogTag, Long> uniformVersions(Set<LogTag> tags, long version) {
        Map<LogTag, Long> m = new LinkedHashMap<>();
        if (tags != null) for (LogTag t : tags) m.put(t, version);
        return m;
    }

    public long seqnum() { return seqnum; }

    /**
     * This entry's position within its {@link #primaryTag()}'s stream, counted from zero.
     *
     * <p>Independent of how many other tags share the physical log, which is what makes
     * it usable as a durable cursor where {@link #seqnum()} is not.
     *
     * <p>For a multi-tag entry, prefer {@link #streamVersion(LogTag)} and name the stream
     * you mean: the primary tag is not stable across JVM runs, so this accessor can
     * return either tag's position from one run to the next.
     */
    public long streamVersion() { return streamVersions.get(primaryTag()); }

    /**
     * This entry's position within {@code tag}'s own stream, or {@link #UNKNOWN_VERSION}
     * if the entry does not carry that tag.
     *
     * <p>This is the cursor a consumer of one stream advances, and the quantity
     * version-keyed reads compare against. For an entry written before per-tag versions
     * existed ({@link #hasPerTagVersions()} {@code == false}) every tag answers with the
     * one number the record holds, which is what such an entry has always reported.
     */
    public long streamVersion(LogTag tag) {
        Long v = streamVersions.get(tag);
        return v == null ? UNKNOWN_VERSION : v;
    }

    /** This entry's position in each stream it belongs to. */
    public Map<LogTag, Long> streamVersions() { return streamVersions; }

    /**
     * Whether the positions this entry reports are genuinely per tag.
     *
     * <p>{@code false} for an entry decoded from a record written before per-tag versions,
     * where one number is reported for every tag because the record cannot say which tag
     * it counted. Always {@code true} for a single-tag entry, whose one number is
     * unambiguous whenever it was written.
     */
    public boolean hasPerTagVersions() { return perTagVersions; }

    /**
     * @deprecated renamed to {@link #streamVersion()}. The name was a fossil of Boki's
     *     per-<em>engine</em> {@code localid}, which is a write-path detail superseded
     *     once the sequencer assigns a seqnum — a different quantity from the permanent
     *     per-<em>tag</em> cursor this actually is. Scheduled for removal.
     */
    @Deprecated(forRemoval = true)
    public long localId() { return streamVersion(); }

    public Set<LogTag> tags() { return tags; }

    /** Returns a defensive copy of the payload. */
    public byte[] data() { return Arrays.copyOf(data, data.length); }

    /** Returns the raw payload without copying; callers must not mutate the returned array. */
    public byte[] dataUnsafe() { return data; }

    public Instant timestamp() { return timestamp; }

    /**
     * Returns the primary tag — the tag whose stream {@link #streamVersion()} counts.
     *
     * <p><strong>Not stable across JVM runs</strong> for a multi-tag entry: {@code tags}
     * is an immutable {@code Set} whose iteration order Java salts per run, so an entry
     * carrying two tags may report either as primary depending on the process that reads
     * it. Single-tag entries — the normal case — are unaffected. Now that each tag has its
     * own position, a multi-tag caller should name its tag rather than rely on this.
     */
    public LogTag primaryTag() {
        return tags.iterator().next();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry e)) return false;
        return seqnum == e.seqnum
                && streamVersions.equals(e.streamVersions)
                && perTagVersions == e.perTagVersions
                && tags.equals(e.tags)
                && Arrays.equals(data, e.data)
                && timestamp.equals(e.timestamp);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(seqnum, streamVersions, perTagVersions, tags, timestamp);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{seqnum=" + seqnum
                + ", streamVersions=" + streamVersions
                + (perTagVersions ? "" : " (legacy, one version for all tags)")
                + ", dataLen=" + data.length
                + ", timestamp=" + timestamp + '}';
    }
}
