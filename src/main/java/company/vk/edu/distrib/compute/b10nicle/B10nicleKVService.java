package company.vk.edu.distrib.compute.b10nicle;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

public class B10nicleKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(B10nicleKVService.class);

    private static final String CONTENT_TYPE_OCTET_STREAM = "application/octet-stream";
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAM = "id";
    private static final String OK_MESSAGE = "OK";
    private static final String CREATED_MESSAGE = "Created";
    private static final String DELETED_MESSAGE = "Deleted";
    private static final String MISSING_ID_MESSAGE = "Missing id parameter";
    private static final String METHOD_NOT_ALLOWED_MESSAGE = "Method not allowed";
    private static final String INTERNAL_ERROR_MESSAGE = "Internal server error";

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private boolean running;

    public B10nicleKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("Service already started");
        }

        try {
            HttpServer localServer = HttpServer.create(new InetSocketAddress(port), 0);
            localServer.createContext(STATUS_PATH, new StatusHandler());
            localServer.createContext(ENTITY_PATH, new EntityHandler());
            localServer.setExecutor(Executors.newCachedThreadPool());
            localServer.start();

            server = localServer;
            running = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to start HTTP server", e);
        }
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }

        running = false;

        HttpServer localServer = server;
        server = null;

        if (localServer != null) {
            localServer.stop(0);
        }

        try {
            dao.close();
        } catch (IOException e) {
            log.error("Error closing DAO", e);
        }
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                sendResponse(exchange, HttpStatus.METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED_MESSAGE);
                return;
            }

            sendResponse(exchange, HttpStatus.OK, OK_MESSAGE);
        }

        private void sendResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
            byte[] response = message.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE_TEXT_PLAIN);
            exchange.sendResponseHeaders(statusCode, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String id = extractId(exchange.getRequestURI().getQuery());

            if (id == null) {
                sendResponse(exchange, HttpStatus.BAD_REQUEST, MISSING_ID_MESSAGE);
                return;
            }

            try {
                processRequest(exchange, id);
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, HttpStatus.BAD_REQUEST, e.getMessage());
            } catch (NoSuchElementException e) {
                sendResponse(exchange, HttpStatus.NOT_FOUND, e.getMessage());
            } catch (IOException e) {
                log.error("IO error handling request", e);
                sendResponse(exchange, HttpStatus.INTERNAL_ERROR, INTERNAL_ERROR_MESSAGE);
            }
        }

        private void processRequest(HttpExchange exchange, String id) throws IOException {
            String method = exchange.getRequestMethod();

            switch (method) {
                case METHOD_GET -> handleGet(exchange, id);
                case METHOD_PUT -> handlePut(exchange, id);
                case METHOD_DELETE -> handleDelete(exchange, id);
                case null, default -> sendResponse(exchange, HttpStatus.METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED_MESSAGE);
            }
        }

        private String extractId(String query) {
            if (query == null || query.isEmpty()) {
                return null;
            }
            String[] params = query.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=");
                if (keyValue.length == 2 && ID_PARAM.equals(keyValue[0])) {
                    return keyValue[1];
                }
            }
            return null;
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] data = dao.get(id);
            sendResponse(exchange, HttpStatus.OK, data);
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] data = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, data);
            sendResponse(exchange, HttpStatus.CREATED, CREATED_MESSAGE.getBytes(StandardCharsets.UTF_8));
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);
            sendResponse(exchange, HttpStatus.ACCEPTED, DELETED_MESSAGE.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, byte[] data) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE_OCTET_STREAM);
        exchange.sendResponseHeaders(statusCode, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
        sendResponse(exchange, statusCode, message.getBytes(StandardCharsets.UTF_8));
    }

    private static final class HttpStatus {
        public static final int OK = 200;
        public static final int CREATED = 201;
        public static final int ACCEPTED = 202;
        public static final int BAD_REQUEST = 400;
        public static final int NOT_FOUND = 404;
        public static final int METHOD_NOT_ALLOWED = 405;
        public static final int INTERNAL_ERROR = 500;
    }
}
