package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.Dao;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryDao<T> implements Dao<T> {
    private final ConcurrentMap<String, T> storage = new ConcurrentHashMap<>();

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException {
        checkKey(key);
        T value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No element with key '%s'".formatted(key));
        }
        return value;
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        checkKey(key);
        storage.remove(key);
    }

    @Override
    public void close() {
        // No resources to close
    }

    private static void checkKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
    }
}
