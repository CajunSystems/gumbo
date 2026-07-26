package com.cajunsystems.gumbo.core;

import java.nio.ByteBuffer;

/**
 * The encoding of a counter held in the tag key-value store: eight bytes, big-endian.
 *
 * <p>The KV stores opaque {@code byte[]}, so a counter only works as a coordination
 * primitive if every writer of a key agrees on how the number is laid out. That agreement
 * has to be stated somewhere, because the alternatives are not interchangeable: an adapter
 * incrementing a value it wrote little-endian, or a client parsing a decimal string, both
 * read a different quantity from the same eight bytes and neither reports an error.
 *
 * <p>Big-endian, fixed width, is the encoding this codebase already uses for every long it
 * writes to storage — the file adapter's record headers, the FDB sequencer's counter — and
 * it keeps a counter's byte order the same on every adapter, which is what lets a value
 * written through one be read through another.
 *
 * <p>A client reading a counter with
 * {@link com.cajunsystems.gumbo.api.LogView#getValue(String)} decodes it with
 * {@link #toLong(byte[])}; one seeding a key before its first increment encodes with
 * {@link #toBytes(long)}.
 */
public final class CounterValues {

    /** Width of an encoded counter, in bytes. */
    public static final int WIDTH = 8;

    private CounterValues() {}

    /** Encodes {@code value} as its eight-byte big-endian representation. */
    public static byte[] toBytes(long value) {
        return ByteBuffer.allocate(WIDTH).putLong(value).array();
    }

    /**
     * Decodes a counter written by {@link #toBytes}. An absent key ({@code null}) reads as
     * {@code 0}, so a counter needs no initialisation before its first increment.
     *
     * @throws IllegalStateException if {@code bytes} is not exactly {@link #WIDTH} bytes —
     *         the key holds something that is not a counter, and returning a number derived
     *         from the wrong bytes would corrupt whatever is counting
     */
    public static long toLong(byte[] bytes) {
        if (bytes == null) return 0L;
        if (bytes.length != WIDTH) {
            throw new IllegalStateException(
                    "not a counter: expected " + WIDTH + " bytes, found " + bytes.length);
        }
        return ByteBuffer.wrap(bytes).getLong();
    }
}
