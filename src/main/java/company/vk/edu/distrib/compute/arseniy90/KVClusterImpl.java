package company.vk.edu.distrib.compute.arseniy90;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;

public class KVClusterImpl implements KVCluster {
    private static final String HOST_COLON = "http://localhost:";
    private static final String NODE_DATA_PATH_PREFIX = "node_";

    private final Map<String, ReplicatedKVServiceImpl> nodes = new ConcurrentHashMap<>();
    private final List<String> endpoints;
    private final Path workingDir;
    private final HashRouter hashRouter;
    private final int replicationFactor;

    public KVClusterImpl(List<Integer> ports, Path workingDir, HashStrategy hashStrategy, int replicationFactor) {
        this.endpoints = ports.stream()
                .sorted()
                .map(p -> HOST_COLON + p)
                .toList();
        this.workingDir = workingDir;
        this.hashRouter = hashStrategy.createRouter(endpoints);
        this.replicationFactor = replicationFactor;
    }

    @Override
    public void start() {
        endpoints.forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        nodes.computeIfAbsent(endpoint, e -> {
            try {
                Path nodePath = workingDir.resolve(NODE_DATA_PATH_PREFIX + e.hashCode());
                Dao<byte[]> dao = new FSDaoImpl(nodePath);
                ReplicatedKVServiceImpl node = new ReplicatedKVServiceImpl(e, replicationFactor, hashRouter, dao);
                node.start();
                return node;
            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to start node " + e, ex);
            }
        });
    }
    
    @Override
    public void stop() {
        nodes.values().forEach(ReplicatedKVServiceImpl::stop);
        nodes.clear();
    }

    @Override
    public void stop(String endpoint) {
        ReplicatedKVServiceImpl node = nodes.remove(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
         return new ArrayList<>(endpoints);
    }
}
