package com.cajunsystems.gumbo.core;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * A per-call token a storage-backed read-modify-write records next to the key it writes, so
 * that a client-library retry can recognise its own commit having landed.
 *
 * <p>FoundationDB's {@code db.run} retries a transaction on {@code commit_unknown_result}: the
 * commit may have applied and the reply been lost. A read-modify-write that simply runs again
 * then does the wrong thing — a compare-and-set reads the value it wrote, finds it is not the
 * expected one, and reports that it lost; a counter is incremented twice; a sequencer skips a
 * number. Writing a record keyed by the token in the same transaction as the value, and
 * looking for the earlier attempts' records first on every retry, turns the retry into a
 * lookup: a record present means the earlier attempt committed, and the result stored in it
 * is returned.
 *
 * <p>The record is keyed by the attempt's <em>read version</em> and then the token, never by
 * the key alone: a single last-writer slot per key is overwritten by whoever writes the key
 * next, and a writer that slips in between a lost commit reply and its retry would make the
 * retry apply again. Keying by read version also gives the records an order, so a writer can
 * clear everything older than a horizon with one range clear and no scan. A retry knows the
 * read versions of its own earlier attempts because they ran in the same call.
 *
 * <p>Tokens are 16 random bytes.
 */
public final class OperationToken {

    /** Width of a token, in bytes. */
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
}
