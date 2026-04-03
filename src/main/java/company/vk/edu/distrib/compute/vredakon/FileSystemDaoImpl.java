package company.vk.edu.distrib.compute.vredakon;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class FileSystemDaoImpl implements Dao<byte[]> {

    private final String storagePath = "./storage/";

    @Override
    public byte[] get(String key) throws IOException {
        return Files.readAllBytes(Path.of(storagePath, key));
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        Files.write(Path.of(storagePath, key), value);
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(Path.of(storagePath, key));
    }

    @Override
    public void close() throws IOException {
        throw new IOException();
    }
}
