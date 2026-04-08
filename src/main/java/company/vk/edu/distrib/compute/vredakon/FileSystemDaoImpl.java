package company.vk.edu.distrib.compute.vredakon;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileSystemDaoImpl implements Dao<byte[]> {

    private static final String STORAGE_PATH = System.getProperty("user.dir")
            + "/src/main/java"
            + "/company/vk/edu/distrib/compute/vredakon/storage/";

    private final Logger log = LoggerFactory.getLogger("FileSystemDao");
    private final AtomicBoolean isClosed;

    public FileSystemDaoImpl() throws IOException {
        createStorage();
        this.isClosed = new AtomicBoolean(false);
    }

    @Override
    public byte[] get(String key) throws IOException {
        if (!isClosed.get()) {
            return Files.readAllBytes(Path.of(STORAGE_PATH, key));
        }
        throw new IllegalStateException("Resource is closed");
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        if (!isClosed.get()) {
            Files.write(Path.of(STORAGE_PATH, key), value);
            return;
        }
        throw new IllegalStateException("Resource is closed");
    }

    @Override
    public void delete(String key) throws IOException {
        if (!isClosed.get()) {
            Files.deleteIfExists(Path.of(STORAGE_PATH, key));
            return;
        }
        throw new IllegalStateException("Resource is closed");
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            return;
        }
        log.error("Resource is already closed");
    }

    private static void createStorage() throws IOException {
        if (Files.notExists(Path.of(STORAGE_PATH))) {
            Files.createDirectory(Path.of(STORAGE_PATH));
        }
    }
}
