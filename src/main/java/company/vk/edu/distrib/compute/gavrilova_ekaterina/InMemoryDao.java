package company.vk.edu.distrib.compute.gavrilova_ekaterina;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return storage.getOrDefault(key, null);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        // No resources to close
    }

}
