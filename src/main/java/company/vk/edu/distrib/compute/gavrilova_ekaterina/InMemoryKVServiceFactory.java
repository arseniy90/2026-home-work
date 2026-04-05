package company.vk.edu.distrib.compute.gavrilova_ekaterina;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class InMemoryKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new InMemoryKVService(port);
    }

}
