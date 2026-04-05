package company.vk.edu.distrib.compute.arseniy90;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

public class ArzhKVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        // return new KVServiceImpl(port, new InMemoryDaoImpl());
        return new KVServiceImpl(port, new FSDaoImpl(Path.of("./dao_data")));
    }   
}
