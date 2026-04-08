package company.vk.edu.distrib.compute.vredakon;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DaoImpl implements Dao<byte[]> {

    private final Logger log = LoggerFactory.getLogger("InMemoryDao");
    private final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private final AtomicBoolean isClosed;

    public DaoImpl() {
        this.isClosed = new AtomicBoolean(false);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        if (isClosed.get()) {
            throw new IllegalStateException("Resource is closed");
        }
        return data.getOrDefault(key, null);
    }

    @Override
    public void delete(String key) {
        if (isClosed.get()) {
            throw new IllegalStateException("Resource is closed");
        }
        data.remove(key);
    }

    @Override
    public void upsert(String key, byte[] value) {
        if (isClosed.get()) {
            throw new IllegalStateException("Resource is closed");
        }
        data.put(key, value);
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            data.clear();
            return;
        }
        log.error("Resource is already closed");
    }
}
