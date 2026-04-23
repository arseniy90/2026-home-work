package company.vk.edu.distrib.compute.arseniy90.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

public class KVServiceImpl implements KVService {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String ID = "id";

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        this.initRoutes();
    }

    private void initRoutes() {
        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if (!"GET".equals(exchange.getRequestMethod()) || !STATUS_PATH.equals(path)) {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }
            exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_OK, -1);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            java.net.URI uri = exchange.getRequestURI();
            if (!ENTITY_PATH.equals(uri.getPath())) {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }

            String id = getId(uri.getQuery());
            if (id == null || id.isBlank()) {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            try {
                processRequests(exchange, id);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_NOT_FOUND, -1);
            } catch (Exception e) {
                log.error("Internal error in request", e);
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_UNAVAILABLE, -1);
            }
        }
    }

    private void processRequests(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> {
                byte[] data = dao.get(id);
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_OK, data.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(data);
                }
            }
            case "PUT" -> {
                byte[] body;
                try (var is = exchange.getRequestBody()) {
                    body = is.readAllBytes();
                }
                dao.upsert(id, body);
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_CREATED, -1);
            }
            case "DELETE" -> {
                dao.delete(id);
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_ACCEPTED, -1);
            }
            default -> {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_BAD_METHOD, -1);
            }
        }
    }

    private static String getId(String query) {
        if (query == null) {
            return null;
        }

        String[] pair = query.split("=");
        if (pair.length == 2 && ID.equals(pair[0])) {
            return pair[1];
        }

        return null;
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Error while closing dao", e);
        }
    }
}
