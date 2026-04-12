package com.central.sharedlog.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;

import java.util.function.Consumer;

/**
 * A {@link LogSerializer} backed by <a href="https://github.com/EsotericSoftware/kryo">Kryo 5</a>.
 *
 * <h2>Thread safety</h2>
 * <p>{@link Kryo} instances are <em>not</em> thread-safe.  This class maintains
 * an internal {@link Pool} so that each concurrent caller borrows its own
 * {@code Kryo} instance without synchronisation overhead.
 *
 * <h2>Class registration</h2>
 * <p>By default, class registration is <em>not</em> required — any serialisable
 * class works out of the box.  For smaller payloads and faster throughput,
 * register classes up front via the {@code registrar} constructor:
 * <pre>{@code
 * LogSerializer<OrderEvent> s = new KryoLogSerializer<>(
 *     OrderEvent.class,
 *     kryo -> {
 *         kryo.register(OrderEvent.class, 10);
 *         kryo.register(OrderStatus.class, 11);
 *     });
 * }</pre>
 *
 * <h2>Java records</h2>
 * <p>Kryo 5.x auto-detects Java records and uses {@code RecordSerializer}
 * which requires no extra configuration.
 *
 * @param <T> the domain type to serialize
 */
public class KryoLogSerializer<T> implements LogSerializer<T> {

    /** Pool capacity: 8 instances; beyond this Kryo objects are GC'd. */
    private static final int POOL_CAPACITY = 8;

    /** Initial output buffer size (bytes); grows automatically. */
    private static final int OUTPUT_INITIAL_CAPACITY = 256;

    private final Class<T> type;
    private final Pool<Kryo> pool;

    /**
     * Creates a serializer with default settings (registration not required).
     * Suitable for most types including Java records and plain classes with
     * a no-arg constructor.
     *
     * @param type the class to serialize/deserialize
     */
    public KryoLogSerializer(Class<T> type) {
        this(type, k -> {});
    }

    /**
     * Creates a serializer with a custom Kryo configurator.  Use this to
     * pre-register classes for smaller payloads.
     *
     * @param type      the class to serialize/deserialize
     * @param registrar called once per pooled {@link Kryo} instance to allow
     *                  class registration and other configuration
     */
    public KryoLogSerializer(Class<T> type, Consumer<Kryo> registrar) {
        this.type = type;
        this.pool = new Pool<>(/* thread safe */ true, /* soft refs */ false, POOL_CAPACITY) {
            @Override
            protected Kryo create() {
                Kryo kryo = new Kryo();
                kryo.setRegistrationRequired(false);
                registrar.accept(kryo);
                return kryo;
            }
        };
    }

    @Override
    public byte[] serialize(T value) {
        Kryo kryo = pool.obtain();
        try (Output output = new Output(OUTPUT_INITIAL_CAPACITY, -1)) {
            kryo.writeObject(output, value);
            return output.toBytes();
        } finally {
            pool.free(kryo);
        }
    }

    @Override
    public T deserialize(byte[] data) {
        Kryo kryo = pool.obtain();
        try (Input input = new Input(data)) {
            return kryo.readObject(input, type);
        } finally {
            pool.free(kryo);
        }
    }
}
