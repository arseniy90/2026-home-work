package company.vk.edu.distrib.compute.arseniy90;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;

public class ReplicatedKVServiceImpl implements ReplicatedService {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String GET_QUERY = "GET";
    private static final String PUT_QUERY = "PUT";
    private static final String DELETE_QUERY = "DELETE";
    private static final String ACK = "ack";
    private static final String ID = "id";
    private static final String INTERNAL_HEADER = "replica";

    private final String currentEndpoint;
    private final int currentPort;
    private final HashRouter hashRouter;
    private final Dao<byte[]> dao;
    private final HttpServer server;
    private final HttpClient client;
    private final int replicationFactor;
    private final Set<Integer> disabledNodes = ConcurrentHashMap.newKeySet();

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private final ExecutorService executor = Executors.newFixedThreadPool(8);

    public ReplicatedKVServiceImpl(String currentEndpoint, int replicationFactor,
        HashRouter hashRouter, Dao<byte[]> dao) throws IOException {
        this.currentEndpoint = currentEndpoint;
        this.replicationFactor = replicationFactor;
        this.hashRouter = hashRouter;
        this.dao = dao;
        this.currentPort = Integer.parseInt(currentEndpoint.substring(currentEndpoint.lastIndexOf(':') + 1));
        this.server = HttpServer.create(new InetSocketAddress(currentPort), 0);
        this.client = HttpClient.newBuilder().executor(executor).connectTimeout(Duration.ofMillis(500)).build();
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
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            URI uri = exchange.getRequestURI();
            if (!ENTITY_PATH.equals(uri.getPath())) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }

            String id = getParam(exchange, ID);
            if (id == null || id.isBlank()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            if (handleInternalRequest(exchange, id)) {
                return;
            }

            String ackParam = getParam(exchange, ACK);
            // log.debug("TEST ack {}", ackParam);
            int ack = (ackParam != null) ? Integer.parseInt(ackParam) : 1;

            if (id == null || ack <= 0 || ack > replicationFactor) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            handleQuorum(exchange, id, ack);
        }
    }

    private boolean handleInternalRequest(HttpExchange exchange, String id) throws IOException {
        if (exchange.getRequestHeaders().containsKey(INTERNAL_HEADER)) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            Response localRes = processRequest(exchange.getRequestMethod(), id, body);
            sendFinalResponse(exchange, localRes);
            return true;
        }
        return false;
    }

    private CompletableFuture<Response> proxyToReplica(String targetEndpoint, String method, String id, byte[] body) {
        if (targetEndpoint.equals(currentEndpoint)) {
            return CompletableFuture.supplyAsync(() -> {
                if (disabledNodes.contains(port())) {
                    return new Response(HttpURLConnection.HTTP_UNAVAILABLE, null);
                }
                return processRequest(method, id, body);
            }, executor);
        }

        HttpRequest.BodyPublisher publisher = body != null
            ? HttpRequest.BodyPublishers.ofByteArray(body) : HttpRequest.BodyPublishers.noBody();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(targetEndpoint + "/v0/entity?id=" + id))
            .header(INTERNAL_HEADER, "replica")
            .method(method, publisher)
            .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(resp -> new Response(resp.statusCode(), resp.body()))
            .exceptionally(e -> new Response(HttpURLConnection.HTTP_UNAVAILABLE, null));
    }

    private void handleQuorum(HttpExchange exchange, String id, int ack) throws IOException {
        List<String> targetEndpoints = hashRouter.getReplicas(id, replicationFactor);
        log.debug("TEST targets {}", targetEndpoints);

        byte[] body = PUT_QUERY.equals(exchange.getRequestMethod())
            ? exchange.getRequestBody().readAllBytes() : null;

        List<CompletableFuture<Response>> futures = targetEndpoints.stream()
            .map(target -> proxyToReplica(target, exchange.getRequestMethod(), id, body))
            .toList();

        List<Response> responses = collectResponses(futures);
        List<Response> validResponses = responses.stream()
                .filter(res -> res.status < HttpURLConnection.HTTP_INTERNAL_ERROR)
                .toList();

        if (validResponses.size() < ack) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, -1);
            return;
        }

        Response bestResponse = validResponses.stream()
                .filter(res -> res.status == HttpURLConnection.HTTP_OK)
                .findFirst()
                .orElse(validResponses.get(0));

        sendFinalResponse(exchange, bestResponse);
    }

    private List<Response> collectResponses(List<CompletableFuture<Response>> futures) {
        return futures.stream()
                .map(f -> {
                    try {
                        return f.get(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        return new Response(HttpURLConnection.HTTP_INTERNAL_ERROR, null);
                    }
                })
                .toList();
    }

    private void sendFinalResponse(HttpExchange exchange, Response result) throws IOException {
        byte[] body = result.body;
        int length = (body != null) ? body.length : -1;
        exchange.sendResponseHeaders(result.status, length);
        if (body != null) {
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }

    private Response processRequest(String method, String id, byte[] body) {
        try {
            switch (method) {
                case GET_QUERY -> {
                    return new Response(HttpURLConnection.HTTP_OK, dao.get(id));
                }
                case PUT_QUERY -> {
                    dao.upsert(id, body);
                    return new Response(HttpURLConnection.HTTP_CREATED, null);
                }
                case DELETE_QUERY -> {
                    dao.delete(id);
                    return new Response(HttpURLConnection.HTTP_ACCEPTED, null);
                }
                default -> {
                    return new Response(HttpURLConnection.HTTP_BAD_METHOD, null);
                }
            }
        } catch (NoSuchElementException e) {
            return new Response(HttpURLConnection.HTTP_NOT_FOUND, null);
        } catch (Exception e) {
            return new Response(HttpURLConnection.HTTP_UNAVAILABLE, null);
        }
    }

    private String getParam(HttpExchange exchange, String key) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || query.isBlank()) {
            return null;
        }

        return Arrays.stream(query.split("&"))
                .filter(param -> param.startsWith(key + "="))
                .map(param -> param.split("=", 2)[1])
                .findFirst()
                .orElse(null);
    }

    @Override public int port() {
        return currentPort;
    }

    @Override public int numberOfReplicas() {
        return replicationFactor;
    }

    @Override public void disableReplica(int nodeId) {
        disabledNodes.add(nodeId);
    }

    @Override public void enableReplica(int nodeId) {
        disabledNodes.remove(nodeId);
    }

    @Override public void start() {
        server.start();
    }

    @Override public void stop() {
        server.stop(0);
        executor.shutdown();
    }

    private record Response(int status, byte[] body) {

    }
}
