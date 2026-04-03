package company.vk.edu.distrib.compute.vredakon;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.Dao;

public class DaoImpl implements Dao<byte[]> {

    private final Map<String, byte[]> data = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        return data.getOrDefault(key, null);
    }

    @Override
    public void delete(String key) {
        data.remove(key);
    }

    @Override
    public void upsert(String key, byte[] value) {
        data.put(key, value);
    }

    @Override
    public void close() throws IOException {
        throw new IOException("Closed");
    }
}
