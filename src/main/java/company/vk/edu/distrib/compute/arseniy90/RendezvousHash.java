package company.vk.edu.distrib.compute.arseniy90;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class RendezvousHash implements HashRouter {
    private static final String MD5 = "MD5";

    private final List<String> endpoints;

    public RendezvousHash(List<String> endpoints) {
        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public String getEndpoint(String key) {
        if (endpoints.isEmpty()) {
            return null;
        }

        String keyEndpoint = null;
        long maxWeight = Long.MIN_VALUE;
        for (String endpoint : endpoints) {
            long weight = calcHash(key, endpoint);
            if (weight > maxWeight) {
                maxWeight = weight;
                keyEndpoint = endpoint;
            }
        }

        return keyEndpoint;
    }

    public long calcHash(String key, String endpoint) {
        try {
            MessageDigest md = MessageDigest.getInstance(MD5);
            md.update(key.getBytes(StandardCharsets.UTF_8));
            md.update(endpoint.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest();
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
