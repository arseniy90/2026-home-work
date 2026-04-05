package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class EntityDao implements Dao<byte[]> {
    private static final Logger log = LoggerFactory.getLogger(EntityDao.class);
    private final Path baseDir;

    public EntityDao(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    public boolean available() {
        try {
            Path probe = baseDir.resolve(".probe");
            Files.write(probe, new byte[]{});
            Files.delete(probe);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isEmpty() || key.isBlank()) {
            throw new IllegalArgumentException();
        }

        try {
            return Files.readAllBytes(baseDir.resolve(key));
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("Entity is not found in storage", e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty() || key.isBlank()) {
            throw new IllegalArgumentException();
        }

        Files.write(baseDir.resolve(key), value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty() || key.isBlank()) {
            throw new IllegalArgumentException();
        }
        try {
            Files.delete(baseDir.resolve(key));
        } catch (NoSuchFileException e) {
            if (log.isWarnEnabled()) {
                log.warn("Entity is not found in storage. Nothing to delete", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // он тут не нужен, т.к. файлы закрываются после каждой операции
    }
}
