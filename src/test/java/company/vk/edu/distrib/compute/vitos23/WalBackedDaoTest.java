package company.vk.edu.distrib.compute.vitos23;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WalBackedDaoTest {

    private static final String TEST_KEY = "key1";

    @TempDir
    private Path tempDir;

    private WalBackedDao dao;
    private String walFilePath;

    @BeforeEach
    void setUp() throws IOException {
        walFilePath = tempDir.resolve("test.wal").toString();
        dao = new WalBackedDao(walFilePath);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (dao != null) {
            dao.close();
        }
    }

    @Test
    void upsertAndGet() throws IOException {
        byte[] value = "test value".getBytes();
        dao.upsert(TEST_KEY, value);

        byte[] retrieved = dao.get(TEST_KEY);

        assertArrayEquals(value, retrieved);
    }

    @Test
    void getAbsentKey() {
        assertThrows(NoSuchElementException.class, () -> dao.get("absent"));
    }

    @Test
    void delete() throws IOException {
        byte[] value = "test value".getBytes();
        dao.upsert(TEST_KEY, value);
        dao.delete(TEST_KEY);

        assertThrows(NoSuchElementException.class, () -> dao.get(TEST_KEY));
    }

    @Test
    void deleteAbsentKey() {
        assertDoesNotThrow(() -> dao.delete("absent"));
    }

    @Test
    void upsertOverwrite() throws IOException {
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        dao.upsert(TEST_KEY, value1);
        dao.upsert(TEST_KEY, value2);

        byte[] retrieved = dao.get(TEST_KEY);

        assertArrayEquals(value2, retrieved);
    }

    @Test
    void upsertEmptyValue() throws IOException {
        byte[] emptyValue = new byte[0];

        dao.upsert(TEST_KEY, emptyValue);

        byte[] retrieved = dao.get(TEST_KEY);

        assertArrayEquals(emptyValue, retrieved);
    }

    @Test
    void upsertNullKey() {
        assertThrows(IllegalArgumentException.class, () -> dao.upsert(null, new byte[1]));
    }

    @Test
    void upsertNullValue() {
        assertThrows(IllegalArgumentException.class, () -> dao.upsert(TEST_KEY, null));
    }

    @Test
    void getNullKey() {
        assertThrows(IllegalArgumentException.class, () -> dao.get(null));
    }

    @Test
    void deleteNullKey() {
        assertThrows(IllegalArgumentException.class, () -> dao.delete(null));
    }

    @Test
    void replayLogAfterClose() throws IOException {
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        dao.upsert(TEST_KEY, value1);
        dao.upsert("key2", value2);
        dao.delete(TEST_KEY);
        dao.close();

        dao = new WalBackedDao(walFilePath);

        assertThrows(NoSuchElementException.class, () -> dao.get(TEST_KEY));
        assertArrayEquals(value2, dao.get("key2"));
    }

    @Test
    void replayLogWithMultipleOperations() throws IOException {
        dao.upsert("a", "1".getBytes());
        dao.upsert("b", "2".getBytes());
        dao.upsert("c", "3".getBytes());
        dao.delete("b");
        dao.upsert("a", "updated".getBytes());
        dao.close();

        dao = new WalBackedDao(walFilePath);

        assertArrayEquals("updated".getBytes(), dao.get("a"));
        assertThrows(NoSuchElementException.class, () -> dao.get("b"));
        assertArrayEquals("3".getBytes(), dao.get("c"));
    }

    @Test
    void closeIdempotent() throws IOException {
        dao.close();
        assertDoesNotThrow(() -> dao.close());
    }

    @Test
    void operationsAfterClose() throws IOException {
        dao.close();

        assertThrows(IllegalStateException.class, () -> dao.get("key"));
        assertThrows(IllegalStateException.class, () -> dao.upsert("key", new byte[1]));
        assertThrows(IllegalStateException.class, () -> dao.delete("key"));
    }

    @Test
    void concurrentUpsert() throws Exception {
        int threadCount = 5;
        int operationsPerThread = 50;
        AtomicInteger errors;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch latch = new CountDownLatch(threadCount);
            errors = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < operationsPerThread; j++) {
                            String key = "key-" + threadId + "-" + j;
                            byte[] value = ("value-" + j).getBytes();
                            dao.upsert(key, value);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            executor.shutdown();
        }

        assertEquals(0, errors.get(), "Concurrent operations should not throw exceptions");
    }

    @Test
    void concurrentUpsertAndGet() throws Exception {
        int threadCount = 3;
        AtomicInteger errors;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            errors = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < 20; j++) {
                            String key = "shared-key-" + (j % 5);
                            byte[] value = ("thread-" + threadId + "-value-" + j).getBytes();
                            dao.upsert(key, value);
                            dao.get(key);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
            executor.shutdown();
        }

        assertEquals(0, errors.get(), "Concurrent upsert/get should not throw exceptions");
    }

    @Test
    void largeValue() throws IOException {
        byte[] largeValue = new byte[10 * 1024];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        dao.upsert("large", largeValue);

        byte[] retrieved = dao.get("large");

        assertArrayEquals(largeValue, retrieved);
    }

    @Test
    void specialCharactersInKey() throws IOException {
        String key = "key-with-special-chars-!@#$%^&*()_+-=[]{}|;':\",./<>?";
        byte[] value = "value".getBytes();

        dao.upsert(key, value);

        byte[] retrieved = dao.get(key);

        assertArrayEquals(value, retrieved);
    }

    @Test
    void unicodeInValue() throws IOException {
        byte[] value = "Привет мир! 你好世界!".getBytes();

        dao.upsert("unicode-key", value);

        byte[] retrieved = dao.get("unicode-key");

        assertArrayEquals(value, retrieved);
    }

    @Test
    void walFileExistsAfterClose() throws IOException {
        dao.upsert(TEST_KEY, "value1".getBytes());
        dao.close();

        Path walPath = Path.of(walFilePath);
        assertTrue(Files.exists(walPath));
        assertTrue(Files.size(walPath) > 0);
    }

    @Test
    void replayEmptyLog() throws IOException {
        dao.close();

        dao = new WalBackedDao(walFilePath);

        assertThrows(NoSuchElementException.class, () -> dao.get("any"));
    }
}
