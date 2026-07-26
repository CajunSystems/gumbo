package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The file adapter is single-writer, and both ways of breaking that rule are silent:
 * two writers assign the same local ids, and the second to close overwrites the first's
 * {@code index.dat}. These tests pin the loud failure that replaces both.
 *
 * @see LogAlreadyOpenException
 */
class FileBasedPersistenceAdapterLockTest {

    private static final LogTag TAG = LogTag.of("orders");

    @TempDir
    Path tempDir;

    @Test
    void secondOpenInSameJvmIsRejected() throws IOException {
        FileBasedPersistenceAdapter first = new FileBasedPersistenceAdapter(tempDir);
        first.open();
        try {
            FileBasedPersistenceAdapter second = new FileBasedPersistenceAdapter(tempDir);
            assertThatThrownBy(second::open)
                    .isInstanceOf(LogAlreadyOpenException.class)
                    .hasMessageContaining(tempDir.toString());
        } finally {
            first.close();
        }
    }

    @Test
    void secondOpenInAnotherProcessIsRejected() throws Exception {
        FileBasedPersistenceAdapter holder = new FileBasedPersistenceAdapter(tempDir);
        holder.open();
        try {
            assertThat(runProbe(tempDir)).isEqualTo(LockProbe.LOCKED);
        } finally {
            holder.close();
        }
        // ...and the lock is genuinely released on close, not just held for the JVM's life.
        assertThat(runProbe(tempDir)).isEqualTo(LockProbe.OPENED);
    }

    @Test
    void lockIsReleasedOnCloseSoTheLogCanBeReopened() throws IOException {
        FileBasedPersistenceAdapter first = new FileBasedPersistenceAdapter(tempDir);
        first.open();
        first.append(new LogEntry(0, 0, Set.of(TAG), "a".getBytes(), Instant.now()));
        first.close();

        FileBasedPersistenceAdapter second = new FileBasedPersistenceAdapter(tempDir);
        assertThatCode(second::open).doesNotThrowAnyException();
        try {
            assertThat(second.readAll()).hasSize(1);
        } finally {
            second.close();
        }
    }

    @Test
    void aFailedOpenDoesNotStrandTheLock() throws IOException {
        // A directory whose log.dat is a directory: open() gets past the lock and then blows up.
        Files.createDirectories(tempDir.resolve("log.dat"));
        FileBasedPersistenceAdapter broken = new FileBasedPersistenceAdapter(tempDir);
        assertThatThrownBy(broken::open).isInstanceOf(IOException.class);

        Files.delete(tempDir.resolve("log.dat"));
        FileBasedPersistenceAdapter retry = new FileBasedPersistenceAdapter(tempDir);
        assertThatCode(retry::open).doesNotThrowAnyException();
        retry.close();
    }

    /**
     * A failure part-way through {@code open()} must close the channels already opened,
     * not only release the lock. The three write channels are opened one at a time, and
     * with {@code open} still false a later {@code close()} returns before reaching them
     * — so anything left behind leaks a descriptor per attempt, invisibly.
     */
    @Test
    void aFailedOpenLeaksNoDescriptors() throws IOException {
        // index.dat as a directory: log.dat's channel is already open when this fails.
        Files.createDirectories(tempDir.resolve("index.dat"));

        long before = openDescriptors();
        for (int i = 0; i < 20; i++) {
            FileBasedPersistenceAdapter broken = new FileBasedPersistenceAdapter(tempDir);
            assertThatThrownBy(broken::open).isInstanceOf(IOException.class);
            broken.close();  // must be a no-op, and must not be what saves us
        }
        long after = openDescriptors();

        assertThat(after - before)
                .as("20 failed opens should leak no descriptors (before=%d after=%d)", before, after)
                .isLessThanOrEqualTo(2);  // slack for unrelated JVM activity
    }

    /** Opening an already-open adapter is a programming error, and must not disturb the lock. */
    @Test
    void openingAnAlreadyOpenAdapterIsRejectedWithoutLosingTheLock() throws Exception {
        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            assertThatThrownBy(adapter::open).isInstanceOf(IllegalStateException.class);

            // The original lock is intact: another process still cannot get in...
            assertThat(runProbe(tempDir)).isEqualTo(LockProbe.LOCKED);
            // ...and the adapter is still usable.
            adapter.append(new LogEntry(0, 0, Set.of(TAG), "a".getBytes(), Instant.now()));
            assertThat(adapter.readAll()).hasSize(1);
        } finally {
            adapter.close();
        }
        // And close() still hands the directory back, rather than the lock outliving it.
        assertThat(runProbe(tempDir)).isEqualTo(LockProbe.OPENED);
    }

    /** Descriptors held by this process, or {@code -1} where the OS does not expose them. */
    private static long openDescriptors() throws IOException {
        Path fd = Path.of("/proc/self/fd");
        if (!Files.isDirectory(fd)) return -1;
        try (var entries = Files.list(fd)) {
            return entries.count();
        }
    }

    /** Runs {@link LockProbe} in a fresh JVM against {@code dir} and returns its exit code. */
    private static int runProbe(Path dir) throws Exception {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        Process p = new ProcessBuilder(
                java.toString(),
                "-cp", System.getProperty("java.class.path"),
                LockProbe.class.getName(),
                dir.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(p.getInputStream().readAllBytes());
        assertThat(p.waitFor(60, TimeUnit.SECONDS))
                .as("probe JVM should exit promptly; output was: %s", output)
                .isTrue();
        return p.exitValue();
    }

    /**
     * Opens the adapter on the directory given as {@code args[0]} and reports the outcome
     * through its exit code. Lives in a separate JVM because file locks are held per
     * process — a same-JVM second open takes a different code path
     * ({@code OverlappingFileLockException}) than a genuine cross-process collision.
     */
    public static final class LockProbe {

        static final int OPENED = 0;
        static final int LOCKED = 3;
        static final int FAILED = 4;

        public static void main(String[] args) {
            FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(Path.of(args[0]));
            try {
                adapter.open();
                List<LogEntry> ignored = adapter.readAll();
                adapter.close();
                System.exit(OPENED);
            } catch (LogAlreadyOpenException e) {
                System.out.println("locked: " + e.getMessage());
                System.exit(LOCKED);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(FAILED);
            }
        }
    }
}
