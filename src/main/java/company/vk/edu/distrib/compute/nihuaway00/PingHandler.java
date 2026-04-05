package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

public class PingHandler implements HttpHandler {

    private final EntityDao entityDao;

    PingHandler(EntityDao dao) {
        this.entityDao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        boolean available = entityDao.available();
        byte[] body = available
                ? "{\"status\": \"ok\"}".getBytes()
                : "{\"status\": \"not available\", \"desc\":\"entity dao not available\"}".getBytes();
        int status = available ? 200 : 503;

        try (exchange) {
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
