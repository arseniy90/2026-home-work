package company.vk.edu.distrib.compute.b10nicle;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();
    private boolean closed;

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);

        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return value.clone();
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);
        validateValue(value);

        storage.put(key, value.clone());
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);

        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        storage.clear();
    }

    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    private void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("DAO is already closed");
        }
    }
}
