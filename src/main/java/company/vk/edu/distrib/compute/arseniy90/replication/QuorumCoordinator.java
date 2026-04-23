package company.vk.edu.distrib.compute.arseniy90.replication;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.arseniy90.model.Response;
import company.vk.edu.distrib.compute.arseniy90.routing.HashRouter;

public class QuorumCoordinator {
    private final HashRouter hashRouter;
    private final ReplicaClient replicaClient;
    private final int replicationFactor;

    // private static final Logger log = LoggerFactory.getLogger(QuorumCoordinator.class);

    public QuorumCoordinator(HashRouter hashRouter, ReplicaClient replicaClient, int replicationFactor) {
        this.hashRouter = hashRouter;
        this.replicaClient = replicaClient;
        this.replicationFactor = replicationFactor;
    }

    public CompletableFuture<Response> coordinateAsync(String method, String id, byte[] body, int ack) {
        List<String> targetEndpoints = hashRouter.getReplicas(id, replicationFactor);
        // log.debug("Target replicas {}", targetEndpoints);

        List<CompletableFuture<Response>> futures = targetEndpoints.stream()
            .map(targetEndpoint -> replicaClient.sendAsync(targetEndpoint, method, id, body))
            .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .handle((v, ex) -> {
                List<Response> responses = futures.stream()
                    .map(f -> f.getNow(new Response(HttpURLConnection.HTTP_INTERNAL_ERROR, null)))
                    .toList();

                List<Response> validResponses = responses.stream()
                    .filter(res -> res.status() < HttpURLConnection.HTTP_INTERNAL_ERROR)
                    .toList();

                if (validResponses.size() < ack) {
                    return new Response(HttpURLConnection.HTTP_UNAVAILABLE, null);
                }

                return validResponses.stream()
                    .filter(res -> res.status() == HttpURLConnection.HTTP_OK)
                    .findFirst()
                    .orElse(validResponses.get(0));
            });
    }
}
