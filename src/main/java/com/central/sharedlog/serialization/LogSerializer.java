package com.central.sharedlog.serialization;

/**
 * Converts objects of type {@code T} to and from {@code byte[]}.
 *
 * <p>Implementations must be <strong>thread-safe</strong>: the shared log
 * can append and read entries concurrently from multiple threads.
 *
 * <p>The {@link KryoLogSerializer} provides an out-of-the-box implementation
 * backed by the Kryo binary serialization framework.  Custom implementations
 * can use JSON, Protobuf, Avro, or any other format.
 *
 * <h2>Example (custom JSON serializer)</h2>
 * <pre>{@code
 * LogSerializer<OrderEvent> json = new LogSerializer<>() {
 *     public byte[] serialize(OrderEvent v)   { return Jackson.toBytes(v); }
 *     public OrderEvent deserialize(byte[] b) { return Jackson.fromBytes(b, OrderEvent.class); }
 * };
 * TypedLogView<OrderEvent> view = service.getTypedView(LogTag.of("orders"), json);
 * }</pre>
 *
 * @param <T> the domain type to serialize
 */
public interface LogSerializer<T> {

    /**
     * Serializes {@code value} to a byte array.
     *
     * @param value the object to serialize; must not be {@code null}
     * @return the binary representation
     */
    byte[] serialize(T value);

    /**
     * Deserializes a byte array back to a {@code T}.
     *
     * @param data the byte array produced by {@link #serialize}
     * @return the reconstructed object; never {@code null} for valid input
     */
    T deserialize(byte[] data);
}
