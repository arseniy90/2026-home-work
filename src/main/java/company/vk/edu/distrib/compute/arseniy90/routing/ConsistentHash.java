package company.vk.edu.distrib.compute.arseniy90.routing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHash implements HashRouter {
    private static final String MD5 = "MD5";
    private static final Integer VIRTUAL_NODES_COUNT = 100;
    private final NavigableMap<Integer, String> ring = new TreeMap<>();
    private final List<String> uniqueNodes;

    public ConsistentHash(List<String> endpoints) {
        for (String endpoint : endpoints) {
            for (int i = 0; i < VIRTUAL_NODES_COUNT; i++) {
                ring.put(calcHash(endpoint + i), endpoint);
            }
        }
        this.uniqueNodes = endpoints.stream().distinct().toList();
    }

    @Override
    public List<String> getReplicas(String key, int cnt) {
        int targetCount = Math.min(cnt, uniqueNodes.size());
        Set<String> replicas = new LinkedHashSet<>(targetCount);
        int hash = calcHash(key);

        NavigableMap<Integer, String> tailMap = ring.tailMap(hash, true);
        for (String endpoint : tailMap.values()) {
            if (replicas.add(endpoint) && replicas.size() == targetCount) {
                return new ArrayList<>(replicas);
            }
        }

        for (String endpoint : ring.values()) {
            if (replicas.add(endpoint) && replicas.size() == targetCount) {
                break;
            }
        }

        return new ArrayList<>(replicas);
    }

    @Override
    public String getEndpoint(String key) {
        return getReplicas(key, 1).get(0);
    }

    private int calcHash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance(MD5);
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest).getInt();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
