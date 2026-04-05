package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/// [Dao] implementation that keeps in memory cache and stores all operations in write-ahead log.
/// Of course, it's very simple and not particularly efficient.
/// Gould persistent key-value storage would be LSM- or B-Tree.
/// This implementation is thread safe.
public class WalBackedDao implements Dao<byte[]> {

    private static final java.nio.charset.Charset KEY_CHARSET = StandardCharsets.UTF_8;

    /// In memory cache. Not thread-safe implementation suffices
    @SuppressWarnings("PMD.UseConcurrentHashMap")
    private final Map<String, byte[]> storage = new HashMap<>();
    /// Read-write lock needed to synchronize all write operations (wal write and cache update should be atomic).
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final RandomAccessFile walFile;

    private boolean closed;

    public WalBackedDao(String filePath) throws IOException {
        this.walFile = new RandomAccessFile(filePath, "rw");
        replayLog();
    }

    /// Restore [#storage] state from WAL.
    /// This method must be called once before usage (e.g. in the constructor).
    private void replayLog() throws IOException {
        lock.writeLock().lock();
        try {
            while (true) {
                int operationOrdinal = walFile.read();
                if (operationOrdinal < 0) {
                    break;
                }
                replayOperation(operationOrdinal);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /// Moved out of [#replayLog()] to fix Codacy's false positive "Avoid instantiating new objects inside loops"
    private void replayOperation(int operationOrdinal) throws IOException {
        Operation operation = Arrays.stream(Operation.values())
                .filter(op -> op.ordinal() == operationOrdinal)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Corrupted wal"));
        String key = new String(readArray(), KEY_CHARSET);
        switch (operation) {
            case SET -> storage.put(key, readArray());
            case DELETE -> storage.remove(key);
        }
    }

    private byte[] readArray() throws IOException {
        int length = walFile.readInt();
        byte[] data = new byte[length];
        walFile.readFully(data);
        return data;
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        checkKey(key);
        lock.readLock().lock();
        try {
            checkOpen();
            byte[] value = storage.get(key);
            if (value == null) {
                throw new NoSuchElementException("No element with key '%s'".formatted(key));
            }
            return value;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        lock.writeLock().lock();
        try {
            checkOpen();
            walFile.write(Operation.SET.ordinal());
            writeArray(key.getBytes(KEY_CHARSET));
            writeArray(value);
            synchronizeWal();
            storage.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        lock.writeLock().lock();
        try {
            checkOpen();
            walFile.write(Operation.DELETE.ordinal());
            writeArray(key.getBytes(KEY_CHARSET));
            synchronizeWal();
            storage.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            walFile.close();
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void writeArray(byte[] data) throws IOException {
        walFile.writeInt(data.length);
        walFile.write(data);
    }

    /// Ensure wal is actually written to disk
    private void synchronizeWal() throws IOException {
        walFile.getFD().sync();
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("Dao is already closed");
        }
    }

    private static void checkKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
    }

    private enum Operation {
        SET,
        DELETE,
    }
}
