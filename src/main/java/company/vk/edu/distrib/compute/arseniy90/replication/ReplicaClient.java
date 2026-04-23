package company.vk.edu.distrib.compute.arseniy90.replication;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.NoSuchElementException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.arseniy90.model.Response;
import company.vk.edu.distrib.compute.arseniy90.stats.StatisticsAggregator;

public class ReplicaClient {
    private static final String GET_QUERY = "GET";
    private static final String PUT_QUERY = "PUT";
    private static final String DELETE_QUERY = "DELETE";
    private static final String INTERNAL_HEADER = "X-Replica-Request";

    private final HttpClient client;
    private final ExecutorService executor;
    private final String currentEndpoint;
    private final Dao<byte[]> dao;
    private final StatisticsAggregator statsAggregator;
    private final Set<Integer> disabledNodes;

    private static final Logger log = LoggerFactory.getLogger(ReplicaClient.class);

    public ReplicaClient(HttpClient client, ExecutorService executor, String currentEndpoint, Dao<byte[]> dao,
           StatisticsAggregator statsAggregator, Set<Integer> disabledNodes) {
        this.client = client;
        this.executor = executor;
        this.currentEndpoint = currentEndpoint;
        this.dao = dao;
        this.statsAggregator = statsAggregator;
        this.disabledNodes = disabledNodes;
    }

    public CompletableFuture<Response> sendAsync(String targetEndpoint, String method, String id, byte[] body) {
        int targetPort = extractPort(targetEndpoint);
        if (disabledNodes.contains(targetPort)) {
            return CompletableFuture.completedFuture(new Response(HttpURLConnection.HTTP_UNAVAILABLE, null));
        }

        if (targetEndpoint.equals(currentEndpoint)) {
            // log.debug("Process local request in {}", targetEndpoint);
            return CompletableFuture.supplyAsync(() -> processLocalRequest(method, id, body), executor);
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

    public Response processLocalRequest(String method, String id, byte[] body) {
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

    public void processLocalStats(HttpExchange exchange, String path) throws IOException {
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

    public void processRemoteStats(HttpExchange exchange, String targetEndpoint, String path) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(targetEndpoint + path))
                .GET()
                .timeout(Duration.ofSeconds(1))
                .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(resp -> new Response(resp.statusCode(), resp.body()))
            .exceptionally(e -> new Response(HttpURLConnection.HTTP_UNAVAILABLE, null))
            .thenAccept(resp -> sendStatResponse(exchange, resp.status(), resp.body()))
            .whenComplete((v, ex) -> {
            if (ex != null) {
                exchange.close();
            }
        });
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

    private static int extractPort(String endpoint) {
        try {
            return Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
        } catch (Exception e) {
            return -1;
        }
    }
}
