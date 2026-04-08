package company.vk.edu.distrib.compute.andeco;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.andeco.controller.EntityController;
import company.vk.edu.distrib.compute.andeco.controller.StatusController;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {

    private final HttpServer server;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        EntityController entityController = new EntityController(dao);
        StatusController statusController = new StatusController();

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(ServerConfigConstants.API_PATH + ServerConfigConstants.ENTITY_PATH,
                entityController::processRequest);
        server.createContext(ServerConfigConstants.API_PATH + ServerConfigConstants.STATUS_PATH,
                statusController::processRequest);
        server.setExecutor(Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors()));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }
}
