package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class HttpUtils {
    private HttpUtils() {
    }

    public static Map<String, String> parseQuery(String query) {
        int partsInEntry = 2;
        if (query == null || query.isBlank()) {
            return Map.of();
        }

        Map<String, String> result = new ConcurrentHashMap<>();
        for (String param : query.split("&")) {
            String[] entry = param.split("=");
            if (entry.length == partsInEntry) {
                result.put(
                        URLDecoder.decode(entry[0], StandardCharsets.UTF_8),
                        URLDecoder.decode(entry[1], StandardCharsets.UTF_8)
                );
            } else {
                result.put(
                        URLDecoder.decode(entry[0], StandardCharsets.UTF_8),
                        ""
                );
            }
        }
        return result;
    }

    public static void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String response = message == null ? "" : message;
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
