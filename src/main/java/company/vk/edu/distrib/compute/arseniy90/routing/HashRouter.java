package company.vk.edu.distrib.compute.arseniy90.routing;

import java.util.List;

public interface HashRouter {
    // Возврщает ноду кластера
    String getEndpoint(String key);

    // Возврщает список нод кластера
    List<String> getReplicas(String key, int cnt);
}
