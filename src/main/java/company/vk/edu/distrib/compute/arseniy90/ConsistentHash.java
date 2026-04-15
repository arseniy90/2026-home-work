package company.vk.edu.distrib.compute.arseniy90;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHash implements HashRouter {
    private static final String MD5 = "MD5";
    private static final Integer VIRTUAL_NODES_COUNT = 100;

    private final NavigableMap<Integer, String> ring = new TreeMap<>();

    public ConsistentHash(List<String> endpoints) {
        for (String enpoint : endpoints) {
            for (int i = 0; i < VIRTUAL_NODES_COUNT; i++) {
                ring.put(calcHash(enpoint + i), enpoint);
            }
        }
    }

    @Override
    public String getEndpoint(String key) {
        if (ring.isEmpty()) {
            return null;
        }

        int hash = calcHash(key);
        Integer enpointKey = ring.ceilingKey(hash);
        if (enpointKey == null) {
            enpointKey = ring.firstKey();
        }
        return ring.get(enpointKey);
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
