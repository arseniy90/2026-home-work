package company.vk.edu.distrib.compute.arseniy90.factory;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import company.vk.edu.distrib.compute.arseniy90.dao.FSDaoImpl;
import company.vk.edu.distrib.compute.arseniy90.service.KVServiceImpl;

public class ArzhKVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        // return new KVServiceImpl(port, new InMemoryDaoImpl());
        return new KVServiceImpl(port, new FSDaoImpl(Path.of("./dao_data")));
    }
}
