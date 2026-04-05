package company.vk.edu.distrib.compute.gavrilova_ekaterina;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;

public class InMemoryKVService implements KVService {

    private static final Logger log = LoggerFactory.getLogger(InMemoryKVService.class);
    private final HttpServer server;
    private final Dao<byte[]> storage;

    public InMemoryKVService(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        //this.storage = new InMemoryDao();
        this.storage = new FileDao(Path.of("ekaterina-gavrilova-storage"));
        initServer();
    }

    @Override
    public void start() {
        server.start();
        log.info("InMemoryKVService started");
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("InMemoryKVService stopped");
    }

    private void initServer() {
        if (server == null) {
            log.error("Server is null");
        } else {
            createContexts();
            log.info("Server is initialized");
        }
    }

    private void createContexts() {
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!Objects.equals(requestMethod, "GET")) {
            sendResponse(exchange, 405, "Method Not Allowed".getBytes());
            return;
        }
        sendResponse(exchange, 200, "OK".getBytes());
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try {
            String id = extractId(exchange);
            if (id == null) {
                sendResponse(exchange, 400, "Missing id parameter".getBytes());
                return;
            }
            String requestMethod = exchange.getRequestMethod();
            switch (requestMethod) {
                case "GET" -> handleGet(exchange, id);
                case "PUT" -> handlePut(exchange, id);
                case "DELETE" -> handleDelete(exchange, id);
                default -> sendResponse(exchange, 405, "Method Not Allowed".getBytes());
            }
        } catch (IOException e) {
            log.error("Error handling /v0/entity", e);
            sendResponse(exchange, 500, "Internal Server Error".getBytes());
        }
    }

    private String extractId(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || !query.startsWith("id=")) {
            return null;
        }
        String id = URLDecoder.decode(query.substring(3), StandardCharsets.UTF_8);
        if (id.isBlank()) {
            return null;
        }
        return id;
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = storage.get(id);
            sendResponse(exchange, 200, value);
        } catch (Exception e) {
            sendResponse(exchange, 404, "Not Found".getBytes());
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (InputStream inputStream = exchange.getRequestBody()) {
            byte[] data = inputStream.readAllBytes();
            storage.upsert(id, data);
            sendResponse(exchange, 201, "Created".getBytes());
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        storage.delete(id);
        sendResponse(exchange, 202, "Accepted".getBytes());
    }

    private void sendResponse(HttpExchange exchange, int statusCode, byte[] bytes) throws IOException {
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

}
