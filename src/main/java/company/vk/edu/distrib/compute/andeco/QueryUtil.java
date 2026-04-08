package company.vk.edu.distrib.compute.andeco;

public final class QueryUtil {
    private static final String ID = "id";

    private QueryUtil() {
    }

    public static String extractId(String query) {
        if (query == null) {
            return null;
        }

        for (String param : query.split("&")) {
            int idx = param.indexOf('=');
            if (idx > 0 && ID.equals(param.substring(0, idx))) {
                return param.substring(idx + 1);
            }
        }
        return null;
    }
}
