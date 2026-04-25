package company.vk.edu.distrib.compute;

public interface ReplicatedService extends KVService {

    /**
     * Порт, на котором запущен сервис -- для удобства тестирования.
     */
    int port();

    /**
     * Фактор репликации: рекомендуется нечетное число и как минимум три узла.
     */
    int numberOfReplicas();

    /**
     * Выключить узел по номеру -- для тестирования кейсов с недоступными репликами.
     */
    void disableReplica(int nodeId);

    /**
     * Включить узел по номеру -- для тестирования кейсов с недоступными репликами.
     */
    void enableReplica(int nodeId);
}
