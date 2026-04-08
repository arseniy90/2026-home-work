package company.vk.edu.distrib.compute.andeco;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class AndecoKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path data = Path.of("..", "data");
        return new KVServiceImpl(port, new FileDao(data));
    }
}
