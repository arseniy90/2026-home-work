package company.vk.edu.distrib.compute.arseniy90.routing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Comparator;

public class RendezvousHash implements HashRouter {
    private static final String MD5 = "MD5";
    private final List<String> endpoints;

    public RendezvousHash(List<String> endpoints) {
        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public String getEndpoint(String key) {
        return getReplicas(key, 1).get(0);
    }

    @Override
    public List<String> getReplicas(String key, int count) {
        if (endpoints.isEmpty()) {
            return List.of();
        }

        return endpoints.stream()
            .sorted(Comparator.comparingLong((String endpoint) -> calcHash(key, endpoint)).reversed())
            .limit(count)
            .toList();
    }

    private long calcHash(String key, String endpoint) {
        try {
            MessageDigest md = MessageDigest.getInstance(MD5);
            md.update(key.getBytes(StandardCharsets.UTF_8));
            md.update(endpoint.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(md.digest()).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
