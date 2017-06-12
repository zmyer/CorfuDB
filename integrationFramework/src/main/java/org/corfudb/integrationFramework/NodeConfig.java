package org.corfudb.integrationFramework;

import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by zlokhandwala on 6/5/17.
 */
@Data
@Builder
public class NodeConfig {

    private boolean single = false;
    private boolean enableTls = false;

    @Builder.Default
    private UUID nodeId = UUID.randomUUID();
    @Builder.Default
    private String address = "localhost";
    @Builder.Default
    private Long port = 9000L;
    @Builder.Default
    private String logLevel = "INFO";

    private String logPath = null;
    private String managementServer = null;
    private Double cacheHeapRatio = null;
    private Long initialToken = null;

    private static final String PORT = "<port>";
    private static final String ADDRESS = "--address";
    private static final String LOG_LEVEL = "--log-level";
    private static final String SINGLE = "--single";
    private static final String LOG_PATH = "--log-path";
    private static final String MEMORY = "--memory";
    private static final String CACHE_HEAP_RATIO = "--cache-heap-ratio";
    private static final String INITIAL_TOKEN = "--initial-token";
    private static final String ENABLE_TLS = "--enable-tls";
    private static final String MANAGEMENT_SERVER = "--management-server";

    public Map<String, String> generateOptionsMap() {
        Map<String, String> options = new HashMap<>();
        options.put(PORT, getPort().toString());
        options.put(ADDRESS, getAddress());
        options.put(LOG_LEVEL, getLogLevel());

        if (isSingle()) {
            options.put(SINGLE, null);
        }

        if (getLogPath() != null) {
            options.put(LOG_PATH, getLogPath());
        } else {
            options.put(MEMORY, null);
        }

        if (getCacheHeapRatio() != null) {
            options.put(CACHE_HEAP_RATIO, getCacheHeapRatio().toString());
        }

        if (getInitialToken() != null) {
            options.put(INITIAL_TOKEN, getInitialToken().toString());
        }

        if (getManagementServer() != null) {
            options.put(MANAGEMENT_SERVER, getManagementServer());
        }

        if (isEnableTls()) {
            options.put(ENABLE_TLS, null);
        }

        return options;
    }

    public String getOptions() {
        Map<String, String> options = generateOptionsMap();
        String port = options.remove(PORT);
        StringBuilder stringBuilder = new StringBuilder();

        options.keySet().forEach(key -> {
            if (options.get(key) != null) {
                stringBuilder.append(key).append("=").append(options.get(key)).append(" ");
            } else {
                stringBuilder.append(key).append(" ");
            }
        });
        stringBuilder.append(port);
        System.out.println(options);
        return stringBuilder.toString();
    }
}
