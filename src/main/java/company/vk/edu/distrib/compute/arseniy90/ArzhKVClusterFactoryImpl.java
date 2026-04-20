package company.vk.edu.distrib.compute.arseniy90;

import java.nio.file.Path;
import java.util.List;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

public class ArzhKVClusterFactoryImpl extends KVClusterFactory {
    private static final int DEFAULT_REPLICATION_FACTOR = 1;

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        // return new KVClusterImpl(ports, Path.of("./cluster_data"), HashStrategy.RENDEZVOUS,
        //  DEFAULT_REPLICATION_FACTOR);
        return new KVClusterImpl(ports, Path.of("./cluster_data"), HashStrategy.CONSISTENT,
            DEFAULT_REPLICATION_FACTOR);
    }
}
