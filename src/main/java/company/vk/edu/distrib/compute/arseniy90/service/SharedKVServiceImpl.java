package company.vk.edu.distrib.compute.arseniy90.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.arseniy90.routing.HashRouter;

public class SharedKVServiceImpl implements KVService {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String GET_QUERY = "GET";
    private static final String PUT_QUERY = "PUT";
    private static final String DELETE_QUERY = "DELETE";
    private static final String ID = "id";

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private final String currentEndpoint;
    private final HashRouter hashRouter;
    private final Dao<byte[]> dao;
    private final HttpServer server;
    private final HttpClient client;

    public SharedKVServiceImpl(String currentEndpoint, HashRouter hashRouter, Dao<byte[]> dao,
            HttpClient client) throws IOException {
        this.currentEndpoint = currentEndpoint;
        this.hashRouter = hashRouter;
        this.dao = dao;
        this.client = client;

        int port = Integer.parseInt(currentEndpoint.substring(currentEndpoint.lastIndexOf(':') + 1));
        server = HttpServer.create(new InetSocketAddress(port), 0);

        initRoutes();
    }

    private void initRoutes() {
        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if (!GET_QUERY.equals(exchange.getRequestMethod()) || !STATUS_PATH.equals(path)) {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }
            exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_OK, -1);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            URI uri = exchange.getRequestURI();
            if (!ENTITY_PATH.equals(uri.getPath())) {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }

            String id = getId(uri.getQuery());
            if (id == null || id.isBlank()) {
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            String targetEndpoint = hashRouter.getEndpoint(id);
            if (currentEndpoint.equals(targetEndpoint)) {
                processLocalRequest(exchange, id);
            } else {
                proxyRequest(exchange, targetEndpoint);
            }
        }
    }

    private void proxyRequest(HttpExchange exchange, String targetEndpoint) throws IOException {
        try {
            URI targetBase = URI.create(targetEndpoint);
            URI requestUri = exchange.getRequestURI();
            URI finalUri = new URI(
                targetBase.getScheme(),
                targetBase.getAuthority(),
                requestUri.getPath(),
                requestUri.getQuery(),
                null
            );

            HttpRequest.BodyPublisher publisher = List.of(PUT_QUERY, DELETE_QUERY).contains(exchange.getRequestMethod())
                ? HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes())
                : HttpRequest.BodyPublishers.noBody();

            HttpRequest request = HttpRequest.newBuilder()
                .uri(finalUri)
                .method(exchange.getRequestMethod(), publisher)
                .build();

            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            int length = (response == null || response.body().length == 0) ? -1 : response.body().length;
            exchange.sendResponseHeaders(response.statusCode(), length);
            if (length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.body());
                }
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_UNAVAILABLE, -1);
        }
    }

    private void processLocalRequest(HttpExchange exchange, String id) throws IOException {
        try {
            processRequest(exchange, id);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_NOT_FOUND, -1);
        } catch (Exception e) {
            log.error("Internal error in request", e);
            exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_UNAVAILABLE, -1);
        }
    }

    private void processRequest(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case GET_QUERY -> {
                byte[] data = dao.get(id);
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_OK, data.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(data);
                }
            }
            case PUT_QUERY -> {
                byte[] body;
                try (var is = exchange.getRequestBody()) {
                    body = is.readAllBytes();
                }
                dao.upsert(id, body);
                exchange.sendResponseHeaders(java.net.HttpURLConnection.HTTP_CREATED, -1);
            }
            case DELETE_QUERY -> {
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
