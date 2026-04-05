package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;

public class EntityHandler implements HttpHandler {

    private final EntityDao dao;

    EntityHandler(EntityDao dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        URI uri = exchange.getRequestURI();
        Map<String, String> params = HttpUtils.parseQuery(uri.getQuery());

        try (exchange) {
            try {
                switch (method) {
                    case "GET" -> {
                        handleGetEntity(exchange, params);
                    }
                    case "PUT" -> {
                        handlePutEntity(exchange, params);
                    }
                    case "DELETE" -> {
                        handleDeleteEntity(exchange, params);
                    }
                    default -> {
                        exchange.close();
                    }
                }
            } catch (NoSuchElementException err) {
                HttpUtils.sendError(exchange, 404, err.getMessage());
            } catch (IllegalArgumentException err) {
                HttpUtils.sendError(exchange, 400, err.getMessage());
            } catch (Exception err) {
                HttpUtils.sendError(exchange, 503, err.getMessage());
            }
        }
    }

    public void handleGetEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, NoSuchElementException, IllegalArgumentException {
        String id = params.get("id");
        byte[] data = dao.get(id);
        exchange.sendResponseHeaders(200, data.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(data);
        }
    }

    public void handlePutEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String id = params.get("id");

        try (InputStream is = exchange.getRequestBody()) {
            var data = is.readAllBytes();
            dao.upsert(id, data);
            exchange.sendResponseHeaders(201, -1);
        }
    }

    public void handleDeleteEntity(HttpExchange exchange, Map<String, String> params)
            throws IOException, IllegalArgumentException {
        String id = params.get("id");
        dao.delete(id);
        exchange.sendResponseHeaders(202, -1);
    }
}
