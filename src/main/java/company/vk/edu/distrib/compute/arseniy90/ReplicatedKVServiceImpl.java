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
import java.nio.charset.StandardCharsets;
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
    private static final String STAT_PATH = "/stats/replica/";
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
    private final StatisticsAggregator statsAggregator;

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
        this.statsAggregator = new StatisticsAggregator();
        initRoutes();
    }

    private void initRoutes() {
        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
        server.createContext(STAT_PATH, this::handleStats);
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
            String id = getParam(exchange, ID);
            if (!isIdValid(exchange, id)) {
                return;
            }

            if (handleInternalRequest(exchange, id)) {
                return;
            }

            int ack = parseAck(exchange);
            if (log.isDebugEnabled()) {
                log.debug("TEST ack {}", ack);
            }

            if (ack < 0) {
                return;
            }

            handleQuorum(exchange, id, ack);
        }
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if (!GET_QUERY.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }

            String[] parts = path.split("/");

            if (log.isDebugEnabled()) {
                log.debug("TEST ack {}", Arrays.toString(parts));
            }

            if (parts.length != 4 || (parts.length == 5 && parts[4] != "access")) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            String targetReplica = parts[3];
            String targetEndpoint = targetReplica.replace("_", ":");

            if (currentEndpoint.equals(targetEndpoint)) {
                handleLocalStats(exchange, path);
                return;
            }

            proxyStatsRequest(exchange, targetEndpoint, path);
        }
    }

    private void proxyStatsRequest(HttpExchange exchange, String targetEndpoint, String path) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + targetEndpoint + path))
                .GET()
                .timeout(Duration.ofSeconds(1))
                .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(resp -> new Response(resp.statusCode(), resp.body()))
            .exceptionally(e -> new Response(HttpURLConnection.HTTP_UNAVAILABLE, null))
            .thenAccept(resp -> sendStatResponse(exchange, resp.status(), resp.body()));
    }

    private void sendStatResponse(HttpExchange exchange, int status, byte[] body) {
        try (exchange) {
            int length = (body != null && body.length > 0) ? body.length : -1;
            exchange.sendResponseHeaders(status, length);
            if (length > 0) {
                exchange.getResponseBody().write(body);
            }
        } catch (IOException e) {
            log.error("Failed to send response", e);
        }
    }

    private void handleLocalStats(HttpExchange exchange, String path) throws IOException {
        String responseBody;
        if (path.endsWith("/access")) {
            responseBody = statsAggregator.getAccessJson();
        } else {
            responseBody = statsAggregator.getKeysJson();
        }

        byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, bytes.length);
        exchange.getResponseBody().write(bytes);
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
                    statsAggregator.trackRead();
                    return new Response(HttpURLConnection.HTTP_OK, dao.get(id));
                }
                case PUT_QUERY -> {
                    statsAggregator.trackWrite(id);
                    dao.upsert(id, body);
                    return new Response(HttpURLConnection.HTTP_CREATED, null);
                }
                case DELETE_QUERY -> {
                    statsAggregator.trackWrite(id);
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

    private boolean isIdValid(HttpExchange exchange, String id) throws IOException {
        if (id == null || id.isBlank()) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            return false;
        }
        return true;
    }

    private int parseAck(HttpExchange exchange) throws IOException {
        String ackParam = getParam(exchange, ACK);
        try {
            int ack = (ackParam != null) ? Integer.parseInt(ackParam) : 1;
            if (ack <= 0 || ack > replicationFactor) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return -1;
            }
            return ack;
        } catch (NumberFormatException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            return -1;
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
