package company.vk.edu.distrib.compute.andeco.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.andeco.Method;
import company.vk.edu.distrib.compute.andeco.QueryUtil;
import company.vk.edu.distrib.compute.andeco.ServerConfigConstants;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.NoSuchElementException;

import static company.vk.edu.distrib.compute.andeco.ServerConfigConstants.CONTENT_TYPE_HEADER;
import static company.vk.edu.distrib.compute.andeco.ServerConfigConstants.OCTET_STREAM;

public class EntityController {

    public static final String REQUEST_MAPPING =
            ServerConfigConstants.API_PATH + ServerConfigConstants.ENTITY_PATH;

    private final Dao<byte[]> dao;

    public EntityController(Dao<byte[]> dao) {
        this.dao = dao;
    }

    public void processRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!REQUEST_MAPPING.equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
                return;
            }

            String id = QueryUtil.extractId(exchange.getRequestURI().getQuery());
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            Method method;
            try {
                method = Method.valueOf(exchange.getRequestMethod());
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }

            switch (method) {
                case GET -> processGet(exchange, id);
                case PUT -> processPut(exchange, id);
                case DELETE -> processDelete(exchange, id);
                default -> exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            }
        }
    }

    private void processGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = dao.get(id);
            exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, OCTET_STREAM);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, value.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        }
    }

    private void processPut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
    }

    private void processDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, -1);
    }
}
