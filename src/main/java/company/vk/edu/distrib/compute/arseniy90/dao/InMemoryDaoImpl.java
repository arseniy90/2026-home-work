package company.vk.edu.distrib.compute.arseniy90.dao;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public class InMemoryDaoImpl implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        checkKey(key);
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key " + key + " was not found");
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        checkKey(key);
        checkValue(value);
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        checkKey(key);
        storage.remove(key);
    }

    @Override
    public void close() {
        storage.clear();
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
