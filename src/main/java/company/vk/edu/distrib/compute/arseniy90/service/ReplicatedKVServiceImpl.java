package company.vk.edu.distrib.compute.arseniy90.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Arrays;
import java.util.Set;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.arseniy90.model.Response;
import company.vk.edu.distrib.compute.arseniy90.replication.QuorumCoordinator;
import company.vk.edu.distrib.compute.arseniy90.replication.ReplicaClient;
import company.vk.edu.distrib.compute.arseniy90.routing.HashRouter;
import company.vk.edu.distrib.compute.arseniy90.stats.StatisticsAggregator;

public class ReplicatedKVServiceImpl implements ReplicatedService {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String STAT_PATH = "/stats/replica/";
    private static final String ACCESS_PATH = "access";
    private static final String GET_QUERY = "GET";
    private static final String PUT_QUERY = "PUT";
    private static final String ACK = "ack";
    private static final String ID = "id";
    private static final String INTERNAL_HEADER = "X-Replica-Request";

    private final String currentEndpoint;
    private final int currentPort;
    private final int replicationFactor;
    private final HttpServer server;
    private final ReplicaClient replicaClient;
    private final QuorumCoordinator coordinator;
    private final StatisticsAggregator statsAggregator;
    private final Set<Integer> disabledNodes = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    public ReplicatedKVServiceImpl(String currentEndpoint, int replicationFactor,
        HashRouter hashRouter, Dao<byte[]> dao) throws IOException {
        this.currentEndpoint = currentEndpoint;
        this.currentPort = Integer.parseInt(currentEndpoint.substring(currentEndpoint.lastIndexOf(':') + 1));
        this.replicationFactor = replicationFactor;
        this.server = HttpServer.create(new InetSocketAddress(currentPort), 0);
        server.setExecutor(executor);
        HttpClient client = HttpClient.newBuilder().executor(executor).connectTimeout(Duration.ofMillis(500)).build();
        this.statsAggregator = new StatisticsAggregator();
        this.replicaClient = new ReplicaClient(client, executor, currentEndpoint, dao, statsAggregator, disabledNodes);
        this.coordinator = new QuorumCoordinator(hashRouter, this.replicaClient, replicationFactor);
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
        String id = getParam(exchange, ID);
        if (!isIdValid(exchange, id)) {
            exchange.close();
            return;
        }

        if (exchange.getRequestHeaders().containsKey(INTERNAL_HEADER)) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            Response resp = replicaClient.processLocalRequest(exchange.getRequestMethod(), id, body);
            sendResponse(exchange, resp);
            return;
        }

        int ack = parseAck(exchange);
        // log.debug("Ack {}", ack);

        if (ack < 0) {
            exchange.close();
            return;
        }

        byte[] body = PUT_QUERY.equals(exchange.getRequestMethod())
            ? exchange.getRequestBody().readAllBytes() : null;

        coordinator.coordinateAsync(exchange.getRequestMethod(), id, body, ack)
            .thenAccept(resp -> {
            try (exchange) {
                sendResponse(exchange, resp);
            } catch (IOException e) {
                log.error("Failed to send response", e);
            }
            })
            .exceptionally(ex -> {
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, -1);
            } catch (IOException e) {
                log.error("Failed to send response on crased coordinator", e);
            }
            return null;
        });
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (!GET_QUERY.equals(exchange.getRequestMethod())) {
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            }
            return;
        }

        String[] parts = path.split("/");
        // log.debug("Stats path parts {}", Arrays.toString(parts));

        if (parts.length != 4 && !(parts.length == 5 && ACCESS_PATH.equals(parts[4]))) {
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            }
            return;
        }

        String targetReplica = parts[3];
        String targetEndpoint = "http://" + targetReplica.replace("_", ":");

        if (currentEndpoint.equals(targetEndpoint)) {
            try (exchange) {
                replicaClient.processLocalStats(exchange, path);
            }
            return;
        }

        replicaClient.processRemoteStats(exchange, targetEndpoint, path);
    }

    private void sendResponse(HttpExchange exchange, Response result) throws IOException {
        try (exchange) {
            byte[] body = result.body();
            int length = (body != null) ? body.length : -1;
            exchange.sendResponseHeaders(result.status(), length);
            if (body != null) {
                try (var os = exchange.getResponseBody()) {
                    os.write(body);
                }
            }
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
}
