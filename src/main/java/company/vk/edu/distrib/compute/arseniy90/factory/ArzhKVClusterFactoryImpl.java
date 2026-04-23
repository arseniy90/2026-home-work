package company.vk.edu.distrib.compute.arseniy90.factory;

import java.nio.file.Path;
import java.util.List;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.arseniy90.model.HashStrategy;
import company.vk.edu.distrib.compute.arseniy90.routing.HashRouter;
import company.vk.edu.distrib.compute.arseniy90.service.KVClusterImpl;

public class ArzhKVClusterFactoryImpl extends KVClusterFactory {
    private static final String HOST_COLON = "http://localhost:";
    private static final int DEFAULT_REPLICATION_FACTOR = 1;

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<String> endpoints = ports.stream()
                .sorted()
                .map(p -> HOST_COLON + p)
                .toList();
        HashRouter hashRouter = HashRouterFactory.createRouter(HashStrategy.CONSISTENT, endpoints);
        // HashRouter hashRouter = HashRouterFactory.createRouter(HashStrategy.RENDEZVOUS, endpoints);

        return new KVClusterImpl(endpoints, Path.of("./cluster_data"), hashRouter, DEFAULT_REPLICATION_FACTOR);
    }
}
