package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

@Disabled
@ParameterizedClass
@ArgumentsSource(KVServiceFactoryArgumentsProvider.class)
public class ReplicationTest extends TestBase {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Parameter
    KVServiceFactory serviceFactory;

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    /**
     * Проверяем, что сервис поддерживает репликацию.
     */
    @Test
    void serviceSupportsReplication() throws IOException {
        assertInstanceOf(ReplicatedService.class, serviceFactory.create(randomPort()),
                "Service does not support replication");
    }

    /**
     * Проверяем, что как есть минимум 3 реплики (фактор репликации).
     */
    @Test
    void replicationFactor() {
        doWithService(service -> assertTrue(service.numberOfReplicas() > 2, "At least 3 replicas expected"));
    }

    /**
     * Ошибка, в случае если параметр ack превышает фактор репликации.
     */
    @Test
    void badAckValue() {
        doWithService(service -> {
            int n = service.numberOfReplicas();
            assertEquals(400, get(endpoint(service.port()), "k", n + 1).statusCode());
        });
    }

    /**
     * Простейший тест: пишем, потом читаем по одному и тому же ключу.
     */
    @Test
    void putAndGet() {
        doWithService(service -> {
            upsert(endpoint(service.port()), "k1", "v1".getBytes(UTF_8));
            String persistedValue = new String(get(endpoint(service.port()), "k1").body(), UTF_8);
            assertEquals("v1", persistedValue);
        });
    }

    /**
     * Запись и чтение по кворуму (все узлы работают).
     */
    @Test
    void quorum() {
        doWithService(service -> {
            int n = service.numberOfReplicas();
            int w = n - 1;
            int r = n - w + 1;
            upsert(endpoint(service.port()), "k2", "v2".getBytes(UTF_8), w);
            String persistedValue = new String(get(endpoint(service.port()), "k2", r).body(), UTF_8);
            assertEquals("v2", persistedValue);
        });
    }

    /**
     * Запись и чтение по кворуму (узлы выходят из строя).
     */
    @Test
    void quorumWithFaultyNodes() {
        doWithService(service -> {
            service.disableReplica(0);
            int n = service.numberOfReplicas();
            int w = n - 1;
            int r = n - 2;
            upsert(endpoint(service.port()), "k3", "v3".getBytes(UTF_8), w);
            service.disableReplica(1);
            String persistedValue = new String(get(endpoint(service.port()), "k3", r).body(), UTF_8);
            assertEquals("v3", persistedValue);
        });
    }

    /**
     * Если на разных узлах есть разные значения по ключу, то читаем самое свежее.
     */
    @Test
    void conflictResolution() {
        doWithService(service -> {
            service.disableReplica(0);
            int n = service.numberOfReplicas();

            // Пишем на все реплики
            upsert(endpoint(service.port()), "k4", "v4_0".getBytes(UTF_8), n);

            // Пишем обновленное значение на все реплики, кроме двух
            service.disableReplica(0);
            service.disableReplica(1);
            upsert(endpoint(service.port()), "k4", "v4_1".getBytes(UTF_8), n - 2);

            // Восстанавливаем упавшие реплики
            service.enableReplica(0);
            service.enableReplica(1);

            // Читаем со всех реплик, ожидаем обновленное значение
            String persistedValue = new String(get(endpoint(service.port()), "k4", n).body(), UTF_8);
            assertEquals("v4_1", persistedValue);
        });
    }

    /**
     * Недостаточно доступных реплик для записи.
     */
    @Test
    void faultyWrite() {
        doWithService(service -> {
            service.disableReplica(0);
            int n = service.numberOfReplicas();
            service.disableReplica(0);
            HttpResponse<Void> result = upsert(endpoint(service.port()), "k5", "v5".getBytes(UTF_8), n);
            assertEquals(500, result.statusCode());
        });
    }

    /**
     * Недостаточно доступных реплик для чтения.
     */
    @Test
    void faultyRead() {
        doWithService(service -> {
            service.disableReplica(0);
            int n = service.numberOfReplicas();
            service.disableReplica(0);
            upsert(endpoint(service.port()), "k6", "v6".getBytes(UTF_8), n - 1);
            HttpResponse<byte[]> result = get(endpoint(service.port()), "k6", n);
            assertEquals(500, result.statusCode());
        });
    }

    /**
     * Пишем, потом удаляем по одному и тому же ключу, не должны читать удаленные данные.
     */
    @Test
    void deletion() {
        doWithService(service -> {
            int n = service.numberOfReplicas();
            upsert(endpoint(service.port()), "k7", "v7".getBytes(UTF_8), n);

            service.disableReplica(1);
            service.disableReplica(2);
            int w = n - 2;
            delete(endpoint(service.port()), "k7", w);

            service.enableReplica(1);
            service.enableReplica(2);
            int r = n - w + 1;
            HttpResponse<byte[]> result = get(endpoint(service.port()), "k7", r);
            assertEquals(404, result.statusCode());
        });
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }

    private void doWithService(ServiceAction action) {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            if (serviceFactory.create(randomPort()) instanceof ReplicatedService service) {
                service.start();
                try {
                    action.perform(service);
                } finally {
                    service.stop();
                }
            }
        });
    }

    @FunctionalInterface
    private interface ServiceAction {

        void perform(ReplicatedService service) throws Exception;
    }
}
