package company.vk.edu.distrib.compute.vredakon;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import company.vk.edu.distrib.compute.Dao;

public class EntityHandler implements HttpHandler {

    private final Dao<byte[]> dao;

    public EntityHandler() throws IOException {
        this.dao = new FileSystemDaoImpl();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {

        String query = exchange.getRequestURI().getQuery();
        Map<String, String> params;

        if (query != null) {
            params = new HashMap<>();

            for (String param : query.split("&")) {
                String[] keyValue = param.split("=");
                try {
                    params.put(keyValue[0], keyValue[1]);
                } catch (ArrayIndexOutOfBoundsException exc) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }
            }

            switch (exchange.getRequestMethod()) {
                case "GET":
                    try (OutputStream os = exchange.getResponseBody()) {
                        byte[] data = dao.get(params.get("id"));
                        exchange.sendResponseHeaders(200, data.length);
                        os.write(data);
                    } catch (Exception e) {
                        exchange.sendResponseHeaders(404, -1);
                    }
                    break;

                case "PUT":
                    dao.upsert(params.get("id"), exchange.getRequestBody().readAllBytes());
                    exchange.sendResponseHeaders(201, -1);
                    break;

                case "DELETE":
                    dao.delete(params.get("id"));
                    exchange.sendResponseHeaders(202, -1);
                    break;

                default:
                    break;
            }
        } else {
            exchange.sendResponseHeaders(400, -1);
        }
    }
}
