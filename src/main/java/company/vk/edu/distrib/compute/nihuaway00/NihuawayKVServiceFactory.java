package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;

public class NihuawayKVServiceFactory extends company.vk.edu.distrib.compute.KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new NihuawayKVService(port);
    }
}
