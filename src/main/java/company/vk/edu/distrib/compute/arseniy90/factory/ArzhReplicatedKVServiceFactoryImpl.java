package company.vk.edu.distrib.compute.arseniy90.factory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.arseniy90.dao.FSDaoImpl;
import company.vk.edu.distrib.compute.arseniy90.model.HashStrategy;
import company.vk.edu.distrib.compute.arseniy90.routing.HashRouter;
import company.vk.edu.distrib.compute.arseniy90.service.ReplicatedKVServiceImpl;

public class ArzhReplicatedKVServiceFactoryImpl extends KVServiceFactory {
    private static final String HOST_COLON = "http://localhost:";
    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        List<String> endpoints = List.of(HOST_COLON + port);
        HashRouter hashRouter = HashRouterFactory.createRouter(HashStrategy.CONSISTENT, endpoints);
        return new ReplicatedKVServiceImpl(endpoints.getFirst(), DEFAULT_REPLICATION_FACTOR, hashRouter,
            new FSDaoImpl(Path.of("./dao_data")));
    }
}
