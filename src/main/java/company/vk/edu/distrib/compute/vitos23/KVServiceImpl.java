package company.vk.edu.distrib.compute.vitos23;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final int NO_BODY_RESPONSE_LENGTH = -1;

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        server.createContext("/v0/status", this::handleStatusCheck);
        server.createContext("/v0/entity", this::dispatchEntityRequest);
    }

    private void handleStatusCheck(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpCodes.OK, NO_BODY_RESPONSE_LENGTH);
        exchange.close();
    }

    @SuppressWarnings("PMD.UseTryWithResources") // Replacement is not possible
    private void dispatchEntityRequest(HttpExchange exchange) throws IOException {
        try {
            handleEntityRequest(exchange);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpCodes.NOT_FOUND, NO_BODY_RESPONSE_LENGTH);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HttpCodes.BAD_REQUEST, NO_BODY_RESPONSE_LENGTH);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to handle request", e);
            }
            exchange.sendResponseHeaders(HttpCodes.INTERNAL_ERROR, NO_BODY_RESPONSE_LENGTH);
        } finally {
            exchange.close();
        }
    }

    private void handleEntityRequest(HttpExchange exchange) throws IOException {
        String id = extractId(exchange.getRequestURI().getQuery());

        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            case null, default ->
                    exchange.sendResponseHeaders(HttpCodes.METHOD_NOT_ALLOWED, NO_BODY_RESPONSE_LENGTH);
        }
    }

    private String extractId(String query) {
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Missing query parameter 'id'");
        }
        String[] params = query.split("&");
        for (String param : params) {
            String[] kv = param.split("=", 2);
            if ("id".equals(kv[0])) {
                if (kv.length < 2 || kv[1].isEmpty()) {
                    throw new IllegalArgumentException("Empty 'id' parameter");
                }
                return kv[1];
            }
        }
        throw new IllegalArgumentException("Missing query parameter 'id'");
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(HttpCodes.OK, value.length);
        exchange.getResponseBody().write(value);
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(HttpCodes.CREATED, NO_BODY_RESPONSE_LENGTH);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(HttpCodes.ACCEPTED, NO_BODY_RESPONSE_LENGTH);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }
}
