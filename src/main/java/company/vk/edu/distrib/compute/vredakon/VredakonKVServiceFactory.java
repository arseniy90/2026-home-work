package company.vk.edu.distrib.compute.vredakon;

import java.io.IOException;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

public class VredakonKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new VredakonKVService(port);
    }
}
