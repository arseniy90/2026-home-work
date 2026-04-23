package company.vk.edu.distrib.compute.arseniy90.factory;

import java.util.List;

import company.vk.edu.distrib.compute.arseniy90.model.HashStrategy;
import company.vk.edu.distrib.compute.arseniy90.routing.ConsistentHash;
import company.vk.edu.distrib.compute.arseniy90.routing.HashRouter;
import company.vk.edu.distrib.compute.arseniy90.routing.RendezvousHash;

public final class HashRouterFactory {
    private HashRouterFactory() {
        throw new IllegalStateException("Utility class");
    }

    public static HashRouter createRouter(HashStrategy strategy, List<String> endpoints) {
        return switch (strategy) {
            case HashStrategy.CONSISTENT -> new ConsistentHash(endpoints);
            case HashStrategy.RENDEZVOUS -> new RendezvousHash(endpoints);
        };
    }
}
