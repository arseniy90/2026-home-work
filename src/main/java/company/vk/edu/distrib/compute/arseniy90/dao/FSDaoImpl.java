package company.vk.edu.distrib.compute.arseniy90.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Map;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public class FSDaoImpl implements Dao<byte[]> {
    private final Path basePath;
    private final Map<String, ReadWriteLock> locks = new ConcurrentHashMap<>();

    public FSDaoImpl(Path basePath) throws IOException {
        this.basePath = basePath.toAbsolutePath().normalize();
        if (!Files.exists(this.basePath)) {
            Files.createDirectories(this.basePath);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        ReadWriteLock lock = getLock(key);
        lock.readLock().lock();
        try {
            Path filePath = getKeyFilePath(key);
            if (!Files.exists(filePath)) {
                throw new NoSuchElementException("Key " + key + " was not found");
            }
            return Files.readAllBytes(filePath);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        checkKey(key);
        checkValue(value);
        ReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        Path tempFile = null;
        try {
            Path filePath = getKeyFilePath(key);
            tempFile = Files.createTempFile(basePath, key, ".tmp");
            Files.write(tempFile, value, StandardOpenOption.WRITE);
            Files.move(tempFile, filePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            if (tempFile != null) {
                Files.deleteIfExists(tempFile);
            }
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        checkKey(key);
        ReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            Path filePath = getKeyFilePath(key);
            Files.deleteIfExists(filePath);
            locks.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        locks.clear();
    }

    private ReadWriteLock getLock(String key) {
        return locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    private Path getKeyFilePath(String key) {
        checkKey(key);
        String resolvedKey = key.replaceFirst("^[^a-zA-Z0-9]+", "");
        Path filePath = basePath.resolve(resolvedKey).normalize();
        if (!filePath.startsWith(basePath)) {
            throw new IllegalArgumentException("resolved path is incorrect for key " + resolvedKey);
        }

        return filePath;
    }

    private void checkKey(String key) throws IllegalArgumentException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is invalid");
        }
    }

    private void checkValue(byte[] value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("value is invalid");
        }
    }
}
