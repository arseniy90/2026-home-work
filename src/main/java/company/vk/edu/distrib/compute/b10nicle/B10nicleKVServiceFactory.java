package company.vk.edu.distrib.compute.b10nicle;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class B10nicleKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new B10nicleKVService(port, new InMemoryDao());
    }
}
