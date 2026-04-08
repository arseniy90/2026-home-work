package company.vk.edu.distrib.compute.vredakon;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VredakonKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(VredakonKVService.class);

    private final HttpServer server;

    public VredakonKVService(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        initServer();
    }

    private void initServer() throws IOException {
        if (server == null) {
            log.error("Server is null");
        } else {
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            log.info("Server initialized");
        }
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
