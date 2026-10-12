package com.cajunsystems.gumbo.core;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * A per-call token a storage-backed read-modify-write records next to the key it writes, so
 * that a client-library retry can recognise its own commit having landed.
 *
 * <p>FoundationDB's {@code db.run} retries a transaction on {@code commit_unknown_result}: the
 * commit may have applied and the reply been lost. A read-modify-write that simply runs again
 * then does the wrong thing — a compare-and-set reads the value it wrote, finds it is not the
 * expected one, and reports that it lost; a counter is incremented twice; a sequencer skips a
 * number. Writing the token in the same transaction as the value, and reading it first on
 * every attempt, turns the retry into a lookup: a stored token equal to this call's means the
 * earlier attempt committed, and the result recorded with it is returned.
 *
 * <p>Tokens are 16 random bytes. A stored record is the token followed by whatever result the
 * operation needs to replay (an 8-byte long for a counter or a sequencer; nothing for a
 * compare-and-set, whose result on a match is simply {@code true}).
 */
public final class OperationToken {

    /** Width of the token prefix of a stored record, in bytes. */
    public static final int WIDTH = 16;

    private OperationToken() {}

    /** A fresh token for one call. */
    public static byte[] fresh() {
        UUID id = UUID.randomUUID();
        return ByteBuffer.allocate(WIDTH)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .array();
    }

    /** Whether {@code stored} (a token, possibly followed by a result) was written by {@code token}'s call. */
    public static boolean matches(byte[] stored, byte[] token) {
        return stored != null && stored.length >= WIDTH && token != null && token.length == WIDTH
                && Arrays.equals(stored, 0, WIDTH, token, 0, WIDTH);
    }

    /** A record of {@code token} followed by {@code result}. */
    public static byte[] withResult(byte[] token, long result) {
        return ByteBuffer.allocate(WIDTH + Long.BYTES).put(token, 0, WIDTH).putLong(result).array();
    }

    /** The result recorded after the token by {@link #withResult}. */
    public static long resultOf(byte[] stored) {
        if (stored == null || stored.length != WIDTH + Long.BYTES) {
            throw new IllegalStateException("operation record has no result: "
                    + (stored == null ? "absent" : stored.length + " bytes"));
        }
        return ByteBuffer.wrap(stored, WIDTH, Long.BYTES).getLong();
    }
}
