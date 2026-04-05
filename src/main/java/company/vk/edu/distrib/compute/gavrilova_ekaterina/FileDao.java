package company.vk.edu.distrib.compute.gavrilova_ekaterina;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Objects;

public class FileDao implements Dao<byte[]> {

    private final Path storageDirectory;

    public FileDao(Path storageDirectory) throws IOException {
        Objects.requireNonNull(storageDirectory);
        if (!Files.exists(storageDirectory)) {
            Files.createDirectories(storageDirectory);
        }
        this.storageDirectory = storageDirectory;
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path file = resolvePath(key);
        try {
            return Files.readAllBytes(file);
        } catch (Exception e) {
            throw new NoSuchElementException(e.initCause(e));
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (value == null) {
            throw new IllegalArgumentException("Value is null");
        }
        Path file = resolvePath(key);
        Files.write(file, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Path file = resolvePath(key);
        Files.deleteIfExists(file);
    }

    @Override
    public void close() throws IOException {
        // No resources to close
    }

    private Path resolvePath(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is null or blank");
        }
        return storageDirectory.resolve(key);
    }

}
