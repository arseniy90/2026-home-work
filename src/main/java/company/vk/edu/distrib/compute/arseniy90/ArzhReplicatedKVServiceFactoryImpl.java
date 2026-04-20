package company.vk.edu.distrib.compute.arseniy90;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

public class ArzhReplicatedKVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        List<String> endpoints = List.of("http://localhost:" + port);
        HashRouter hashRouter = HashStrategy.CONSISTENT.createRouter(endpoints);
        return new ReplicatedKVServiceImpl(endpoints.getFirst(), 3, hashRouter,
            new FSDaoImpl(Path.of("./dao_data")));
    }   
}
