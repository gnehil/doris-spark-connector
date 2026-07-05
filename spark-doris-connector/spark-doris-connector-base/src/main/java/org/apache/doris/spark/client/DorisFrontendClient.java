// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.client;

import org.apache.doris.spark.client.entity.Backend;
import org.apache.doris.spark.client.entity.DorisColumnStats;
import org.apache.doris.spark.client.entity.DorisTableStats;
import org.apache.doris.spark.client.entity.Frontend;
import org.apache.doris.spark.client.stats.DorisStatsCache;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.OptionRequiredException;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.QueryPlan;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.HttpUtil;
import org.apache.doris.spark.util.HttpUtils;
import org.apache.doris.spark.util.LoadBalanceList;
import org.apache.doris.spark.util.URLs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DorisFrontendClient implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisFrontendClient.class);

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();
    private static final String MANAGER_BACKENDS = "/rest/v2/manager/node/backends";
    private static final String COMPUTE_GROUP_NAME = "compute_group_name";
    private static final String CLOUD_CLUSTER_NAME = "cloud_cluster_name";

    private final DorisConfig config;
    private final String username;
    private final String password;
    private final LoadBalanceList<Frontend> frontends;
    private final boolean isHttpsEnabled;
    private transient CloseableHttpClient httpClient;

    public DorisFrontendClient() {
        this.config = null;
        this.username = null;
        this.password = null;
        this.httpClient = null;
        this.isHttpsEnabled = false;
        this.frontends = new LoadBalanceList<>(Collections.emptyList());
    }

    public DorisFrontendClient(DorisConfig config) throws Exception {
        this.config = config;
        this.username = config.getValue(DorisOptions.DORIS_USER);
        this.password = config.getValue(DorisOptions.DORIS_PASSWORD);
        this.isHttpsEnabled = config.getValue(DorisOptions.DORIS_ENABLE_HTTPS);
        this.frontends = initFrontends(config);
    }

    private LoadBalanceList<Frontend> initFrontends(DorisConfig config) throws Exception {
        String frontendNodes = config.getValue(DorisOptions.DORIS_FENODES);
        String[] frontendNodeArray = frontendNodes.split(",");
        List<Frontend> frontendList = null;
        if (config.getValue(DorisOptions.DORIS_FE_AUTO_FETCH)) {
            Exception ex = null;
            for (String frontendNode : frontendNodeArray) {
                String[] nodeDetails = frontendNode.split(":");
                try {
                    LoadBalanceList<Frontend> list = new LoadBalanceList<>(
                        Collections.singletonList(new Frontend(nodeDetails[0],
                            nodeDetails.length > 1 ? Integer.parseInt(nodeDetails[1]) : -1)));
                    frontendList = requestFrontends(list, (frontend, client) -> {
                        String url = URLs.getFrontEndNodes(frontend.getHost(), frontend.getHttpPort(),
                                isHttpsEnabled);
                        HttpGet httpGet = new HttpGet(url);
                        HttpUtils.setAuth(httpGet, username, password);
                        JsonNode dataNode;
                        try {
                            HttpResponse response = client.execute(httpGet);
                            dataNode = extractDataFromResponse(response, url);
                        } catch (IOException e) {
                            throw new RuntimeException("fetch fe failed", e);
                        }
                        ArrayNode columnNames = (ArrayNode) dataNode.get("columnNames");
                        ArrayNode rows = (ArrayNode) dataNode.get("rows");
                        return parseFrontends(columnNames, rows);
                    });
                } catch (Exception e) {
                    LOG.warn("fetch fe request on {} failed, err: {}", frontendNode, e.getMessage());
                    ex = e;
                }
            }
            if (frontendList == null || frontendList.isEmpty()) {
                if (ex == null) {
                    throw new DorisException("frontend init fetch failed, empty frontend list");
                }
                throw new DorisException("frontend init fetch failed", ex);
            }
            return new LoadBalanceList<>(frontendList);
        } else {
            int queryPort = config.contains(DorisOptions.DORIS_QUERY_PORT) ?
                    config.getValue(DorisOptions.DORIS_QUERY_PORT) : -1;
            int flightSqlPort = config.contains(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT) ?
                    config.getValue(DorisOptions.DORIS_READ_FLIGHT_SQL_PORT) : -1;
            return new LoadBalanceList<>(Arrays.stream(frontendNodeArray)
                    .map(node -> {
                        String[] nodeParts = node.split(":");
                        return new Frontend(nodeParts[0], nodeParts.length > 1 ? Integer.parseInt(nodeParts[1]) : -1, queryPort, flightSqlPort);
                    })
                    .collect(Collectors.toList()));
        }
    }

    public <T> T requestFrontends(BiFunction<Frontend, CloseableHttpClient, T> reqFunc) throws Exception {
        return requestFrontends(frontends, reqFunc);
    }

    private <T> T requestFrontends(LoadBalanceList<Frontend> frontEnds, BiFunction<Frontend, CloseableHttpClient, T> reqFunc) throws Exception {
        if (httpClient == null) {
            httpClient = HttpUtils.getHttpClient(config);
        }
        Exception ex = null;
        for (Frontend frontEnd : frontEnds) {
            try {
                if(HttpUtil.tryHttpConnection(frontEnd.hostHttpPortString())){
                    return reqFunc.apply(frontEnd, httpClient);
                }
            } catch (Exception e) {
                LOG.warn("fe http request on {} failed, err: {}", frontEnd.hostHttpPortString(), e.getMessage());
                frontEnds.reportFailed(frontEnd);
                ex = e;
            }
        }
        if (ex == null) {
            ex = new Exception("All frontends failed to execute request.");
        }
        throw ex;
    }

    public <T> T queryFrontends(Function<Connection, T> function) throws Exception {
        Exception ex = null;
        for (Frontend frontEnd : frontends) {
            if (frontEnd.getQueryPort() == -1) {
                ex = new OptionRequiredException(DorisOptions.DORIS_QUERY_PORT.getName());
                break;
            }
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                Class.forName("com.mysql.jdbc.Driver");
            }
            try (Connection conn = DriverManager.getConnection("jdbc:mysql://" + frontEnd.getHost() + ":" + frontEnd.getQueryPort(), username, password)) {
                return function.apply(conn);
            } catch (SQLException e) {
                LOG.warn("fe jdbc query on {} failed, err: {}", frontEnd.hostQueryPortString(), e.getMessage());
                ex = e;
            }
        }
        throw ex;
    }

    public List<Pair<String[], String>> listTables(String[] databases) throws Exception {
        return queryFrontends(conn -> {
            String where = databases.length == 1 ? " WHERE TABLE_SCHEMA = '" + databases[0] + "'" : "";
            String sql = "SELECT TABLE_NAME FROM `information_schema`.`tables`" + where;
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                List<Pair<String[], String>> result = new ArrayList<>();
                while (resultSet.next()) {
                    result.add(Pair.of(databases, resultSet.getString(1)));
                }
                return result;
            } catch (SQLException e) {
                throw new RuntimeException("list tables query failed", e);
            }
        });
    }

    public String[] listDatabases() throws Exception {
        return queryFrontends(conn -> {
            String sql = "SELECT SCHEMA_NAME FROM `information_schema`.`schemata`";
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                List<String> result = new ArrayList<>();
                while (resultSet.next()) {
                    String schemaName = resultSet.getString(1);
                    if (!"information_schema".equals(schemaName)) {
                        result.add(schemaName);
                    }
                }
                return result.toArray(new String[0]);
            } catch (SQLException e) {
                throw new RuntimeException("list databases query failed", e);
            }
        });
    }

    public boolean databaseExists(String database) throws Exception {
        if (StringUtils.isBlank(database)) {
            return false;
        }
        return queryFrontends(conn -> {
            String sql = "SELECT SCHEMA_NAME FROM `information_schema`.`schemata` WHERE SCHEMA_NAME = '" + database + "'";
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    if (resultSet.getString(1).equals(database)) {
                        return true;
                    }
                }
                return false;
            } catch (SQLException e) {
                throw new RuntimeException("check databases exists query failed", e);
            }
        });
    }

    public Schema getTableSchema(String db, String table) throws Exception {
        return requestFrontends((frontend, httpClient) -> {
            String url = URLs.tableSchema(frontend.getHost(), frontend.getHttpPort(), db, table, isHttpsEnabled);
            HttpGet httpGet = new HttpGet(url);
            HttpUtils.setAuth(httpGet, username, password);
            Schema dorisSchema;
            try {
                HttpResponse response = httpClient.execute(httpGet);
                JsonNode dataNode = extractDataFromResponse(response, url);
                dorisSchema = MAPPER.readValue(dataNode.traverse(), Schema.class);
            } catch (IOException e) {
                throw new RuntimeException("table schema request failed", e);
            }
            return dorisSchema;
        });
    }

    private List<Frontend> parseFrontends(ArrayNode columnNames, ArrayNode rows) {
        int hostIdx = -1;
        int httpPortIdx = -1;
        int queryPortIdx = -1;
        int flightSqlIdx = -1;
        for (int idx = 0; idx < columnNames.size(); idx++) {
            String columnName = columnNames.get(idx).asText();
            switch (columnName) {
                case "Host":
                case "HostName":
                    hostIdx = idx;
                    break;
                case "HttpPort":
                    httpPortIdx = idx;
                    break;
                case "QueryPort":
                    queryPortIdx = idx;
                    break;
                case "ArrowFlightSqlPort":
                    flightSqlIdx = idx;
                    break;
                default:
                    break;
            }
        }
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<Frontend> frontends = new ArrayList<>();
        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            ArrayNode row = (ArrayNode) rows.get(rowIdx);
            if (flightSqlIdx == -1) {
                frontends.add(new Frontend(row.get(hostIdx).asText(), row.get(httpPortIdx).asInt(), row.get(queryPortIdx).asInt()));
            } else {
                frontends.add(new Frontend(row.get(hostIdx).asText(), row.get(httpPortIdx).asInt(), row.get(queryPortIdx).asInt(), row.get(flightSqlIdx).asInt()));
            }
        }
        return frontends;
    }

    public QueryPlan getQueryPlan(String database, String table, String sql) throws Exception {
        return requestFrontends((frontend, httpClient) -> {
            try {
                String url = URLs.queryPlan(frontend.getHost(), frontend.getHttpPort(), database, table, isHttpsEnabled);
                HttpPost httpPost = new HttpPost(url);
                HttpUtils.setAuth(httpPost, username, password);
                String body = MAPPER.writeValueAsString(ImmutableMap.of("sql", sql));
                StringEntity stringEntity = new StringEntity(body, StandardCharsets.UTF_8);
                stringEntity.setContentEncoding("UTF-8");
                stringEntity.setContentType("application/json");
                httpPost.setEntity(stringEntity);
                HttpResponse response = httpClient.execute(httpPost);
                JsonNode dataJsonNode = extractDataFromResponse(response, url);
                if (dataJsonNode.get("exception") != null) {
                    throw new DorisException("query plan failed, exception: " + dataJsonNode.get("exception").asText());
                }
                return MAPPER.readValue(dataJsonNode.traverse(), QueryPlan.class);
            } catch (Exception e) {
                throw new RuntimeException("query plan request failed", e);
            }
        });
    }


    private JsonNode extractDataFromResponse(HttpResponse response, String url) throws IOException {
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new RuntimeException("request fe with url: [" + url + "] failed with http code: "
                    + response.getStatusLine().getStatusCode() + ", reason: "
                    + response.getStatusLine().getReasonPhrase());
        }
        String entity = EntityUtils.toString(response.getEntity());
        JsonNode respNode = MAPPER.readTree(entity);
        String code = respNode.get("code").asText();
        if (!"0".equalsIgnoreCase(code)) {
            throw new RuntimeException("fetch fe url:[" + url + "] failed with invalid msg code, response: " + entity);
        }
        return respNode.get("data");
    }

    public String[] getTableAllColumns(String db, String table) throws Exception {
        Schema tableSchema = getTableSchema(db, table);
        return tableSchema.getProperties().stream().map(Field::getName).toArray(String[]::new);
    }

    public List<Backend> getAliveBackends() throws Exception {
        return getAliveBackends(null);
    }

    public List<Backend> getAliveBackends(String computeGroupName) throws Exception {
        if (StringUtils.isNotBlank(computeGroupName)) {
            try {
                return getManagerBackends(computeGroupName);
            } catch (Exception e) {
                LOG.warn("Failed to get backends via /rest/v2/manager/node/backends for compute group '{}', "
                        + "falling back to standard backends API. Error: {}", computeGroupName, e.getMessage());
            }
        }
        return requestFrontends((frontend, client) -> {
            String url = URLs.aliveBackend(frontend.getHost(), frontend.getHttpPort(), isHttpsEnabled);
            HttpGet httpGet = new HttpGet(url);
            HttpUtils.setAuth(httpGet, username, password);
            ArrayNode backendsNode;
            try {
                CloseableHttpResponse res = client.execute(httpGet);
                JsonNode dataNode = extractDataFromResponse(res, url);
                backendsNode = (ArrayNode) dataNode.get("backends");
            } catch (IOException e) {
                throw new RuntimeException("get alive backends failed", e);
            }
            List<Backend> backends = new ArrayList<>();
            for (JsonNode backendNode : backendsNode) {
                if ("true".equalsIgnoreCase(backendNode.get("is_alive").asText())) {
                    backends.add(new Backend(backendNode.get("ip").asText(), backendNode.get("http_port").asInt(), -1));
                }
            }
            Collections.shuffle(backends);
            return backends;
        });
    }

    private List<Backend> getManagerBackends(String computeGroupName) throws Exception {
        return requestFrontends((frontend, client) -> {
            String url = URLs.managerBackends(frontend.getHost(), frontend.getHttpPort(), isHttpsEnabled);
            HttpGet httpGet = new HttpGet(url);
            HttpUtils.setAuth(httpGet, username, password);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                    throw new RuntimeException("request fe with url: [" + url + "] failed with http code: "
                            + response.getStatusLine().getStatusCode() + ", reason: "
                            + response.getStatusLine().getReasonPhrase());
                }
                String entity = EntityUtils.toString(response.getEntity());
                List<Backend> backends = parseManagerBackends(entity, computeGroupName);
                Collections.shuffle(backends);
                return backends;
            } catch (IOException e) {
                throw new RuntimeException("get manager backends failed", e);
            }
        });
    }

    static List<Backend> parseManagerBackends(String response, String computeGroupName) {
        if (StringUtils.isBlank(computeGroupName)) {
            throw managerBackendsException(computeGroupName, "compute group is empty");
        }

        JsonNode rootNode;
        try {
            rootNode = MAPPER.readTree(response);
        } catch (IOException e) {
            throw managerBackendsException(computeGroupName, "Parse Doris manager backend response to json failed. res: " + response);
        }

        JsonNode dataNode = unwrapManagerBackendData(rootNode, computeGroupName);
        JsonNode columnNode = dataNode.path("columnNames");
        if (!columnNode.isArray()) {
            columnNode = dataNode.path("column_names");
        }
        JsonNode rowNode = dataNode.path("rows");
        if (!columnNode.isArray() || !rowNode.isArray()) {
            throw managerBackendsException(computeGroupName, "response does not contain columnNames/column_names and rows");
        }

        Map<String, Integer> columnIndexes = getColumnIndexes(columnNode, computeGroupName);
        int hostIndex = requireColumn(columnIndexes, "Host", computeGroupName);
        int httpPortIndex = requireColumn(columnIndexes, "HttpPort", computeGroupName);
        int aliveIndex = requireColumn(columnIndexes, "Alive", computeGroupName);
        int tagIndex = requireColumn(columnIndexes, "Tag", computeGroupName);

        List<Backend> backends = new ArrayList<>();
        for (JsonNode row : rowNode) {
            if (!row.isArray()) {
                throw managerBackendsException(computeGroupName, "backend row is not an array");
            }
            if (!Boolean.parseBoolean(getManagerBackendCell(row, aliveIndex))) {
                continue;
            }
            String rowComputeGroupName = getComputeGroupNameFromTag(getManagerBackendCell(row, tagIndex));
            if (!computeGroupName.equals(rowComputeGroupName)) {
                continue;
            }
            String httpPort = getManagerBackendCell(row, httpPortIndex);
            try {
                backends.add(new Backend(getManagerBackendCell(row, hostIndex), Integer.parseInt(httpPort), -1));
            } catch (NumberFormatException e) {
                throw managerBackendsException(computeGroupName, "backend HttpPort is invalid: " + httpPort);
            }
        }

        if (backends.isEmpty()) {
            throw managerBackendsException(computeGroupName,
                    "no alive backend found. If the target is a virtual compute group, configure its physical active compute group");
        }
        return backends;
    }

    private static JsonNode unwrapManagerBackendData(JsonNode rootNode, String computeGroupName) {
        if (rootNode.has("code") && rootNode.has("msg")) {
            if (!"0".equalsIgnoreCase(rootNode.path("code").asText())) {
                throw managerBackendsException(computeGroupName,
                        rootNode.path("msg").asText() + ": " + rootNode.path("data").asText());
            }
            return rootNode.path("data");
        }
        return rootNode;
    }

    private static Map<String, Integer> getColumnIndexes(JsonNode columnNode, String computeGroupName) {
        Map<String, Integer> columnIndexes = new HashMap<>();
        for (int i = 0; i < columnNode.size(); i++) {
            String columnName = columnNode.get(i).asText();
            if (StringUtils.isNotBlank(columnName)) {
                columnIndexes.put(columnName.toLowerCase(), i);
            }
        }
        if (columnIndexes.isEmpty()) {
            throw managerBackendsException(computeGroupName, "backend columns are empty");
        }
        return columnIndexes;
    }

    private static int requireColumn(Map<String, Integer> columnIndexes, String columnName, String computeGroupName) {
        Integer index = columnIndexes.get(columnName.toLowerCase());
        if (index == null) {
            throw managerBackendsException(computeGroupName, "backend response missing required column " + columnName);
        }
        return index;
    }

    private static String getManagerBackendCell(JsonNode row, int index) {
        JsonNode cell = row.get(index);
        if (cell == null || cell.isNull()) {
            return "";
        }
        return cell.asText();
    }

    static String getComputeGroupNameFromTag(String tag) {
        Map<String, String> tagMap = parseBackendTag(tag);
        String computeGroupName = tagMap.get(COMPUTE_GROUP_NAME);
        if (StringUtils.isNotBlank(computeGroupName)) {
            return computeGroupName;
        }
        return tagMap.get(CLOUD_CLUSTER_NAME);
    }

    private static Map<String, String> parseBackendTag(String tag) {
        Map<String, String> tagMap = new HashMap<>();
        if (StringUtils.isBlank(tag)) {
            return tagMap;
        }

        try {
            JsonNode tagNode = MAPPER.readTree(tag);
            if (tagNode.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = tagNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    tagMap.put(entry.getKey(), entry.getValue().asText());
                }
                return tagMap;
            }
        } catch (IOException e) {
            // Fall through to parse Doris PrintableMap style tag strings.
        }

        String tagContent = tag.trim();
        if (tagContent.startsWith("{") && tagContent.endsWith("}")) {
            tagContent = tagContent.substring(1, tagContent.length() - 1);
        }
        for (String entry : tagContent.split(",")) {
            String[] keyValue = entry.split(":", 2);
            if (keyValue.length != 2) {
                continue;
            }
            tagMap.put(stripQuote(keyValue[0]), stripQuote(keyValue[1]));
        }
        return tagMap;
    }

    private static String stripQuote(String value) {
        String result = value.trim();
        if (result.length() >= 2) {
            char first = result.charAt(0);
            char last = result.charAt(result.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return result.substring(1, result.length() - 1);
            }
        }
        return result;
    }

    private static RuntimeException managerBackendsException(String computeGroupName, String reason) {
        return new RuntimeException(String.format(
                "Failed to get backends for compute group '%s' from %s: %s. Required privileges: information_schema SELECT on Doris 3.x/4.x, or ADMIN on Doris 2.1.",
                computeGroupName, MANAGER_BACKENDS, reason));
    }

    public void truncateTable(String database, String table) throws Exception {
        queryFrontends(conn -> {
            String sql = "TRUNCATE TABLE " + database + "." + table;
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                preparedStatement.execute();
                return null;
            } catch (SQLException e) {
                throw new RuntimeException("truncate table failed", e);
            }
        });
    }

    public LoadBalanceList<Frontend> getFrontends() {
        return frontends;
    }

    // ------------------------------------------------------------------
    // Statistics
    // ------------------------------------------------------------------

    /**
     * Fetch table-level statistics via {@code SHOW TABLE STATS}.
     *
     * <p>Uses a dedicated JDBC connection with connect/socket timeouts from stats options.
     * Results are cached in {@link DorisStatsCache}. On any failure, returns
     * {@link Optional#empty()} and caches it to avoid repeated warnings within TTL.
     */
    public Optional<DorisTableStats> fetchTableStats(String db, String table) {
        if (!isStatsAvailable()) {
            return Optional.empty();
        }
        String feKey = buildFeKey();
        String key = DorisStatsCache.tableKey(feKey, db, table);
        long ttlMs;
        try {
            ttlMs = config.getValue(DorisOptions.DORIS_STATS_CACHE_TTL_MS);
        } catch (OptionRequiredException e) {
            return Optional.empty();
        }
        DorisStatsCache cache = DorisStatsCache.getInstance(ttlMs);
        Optional<DorisTableStats> cached = cache.getTableStats(key);
        if (cached != null) {
            return cached;
        }
        Optional<DorisTableStats> result;
        try {
            result = queryFrontendsForStats(conn -> {
                try {
                    return fetchTableStatsInternal(conn, db, table);
                } catch (SQLException e) {
                    throw new RuntimeException("fetch table stats failed", e);
                }
            });
        } catch (Exception e) {
            LOG.warn("fetch table stats for {}.{} failed (stats reporting will be skipped): {}", db, table, e.getMessage());
            result = Optional.empty();
        }
        cache.putTableStats(key, result);
        return result;
    }

    /**
     * Fetch column-level statistics via {@code SHOW COLUMN STATS}.
     *
     * <p>When {@code cols} is non-empty, only those columns are requested. Results are cached
     * in {@link DorisStatsCache}. On any failure, returns an empty map and caches it.
     */
    public Map<String, DorisColumnStats> fetchColumnStats(String db, String table, List<String> cols) {
        if (!isStatsAvailable()) {
            return Collections.emptyMap();
        }
        String feKey = buildFeKey();
        List<String> sorted = cols == null ? Collections.emptyList() : cols.stream().sorted().collect(Collectors.toList());
        int colsHash = sorted.hashCode();
        String key = DorisStatsCache.columnKey(feKey, db, table, colsHash);
        long ttlMs;
        try {
            ttlMs = config.getValue(DorisOptions.DORIS_STATS_CACHE_TTL_MS);
        } catch (OptionRequiredException e) {
            return Collections.emptyMap();
        }
        DorisStatsCache cache = DorisStatsCache.getInstance(ttlMs);
        Map<String, DorisColumnStats> cached = cache.getColumnStats(key);
        if (cached != null) {
            return cached;
        }
        Map<String, DorisColumnStats> result;
        try {
            result = queryFrontendsForStats(conn -> {
                try {
                    return fetchColumnStatsInternal(conn, db, table, sorted);
                } catch (SQLException e) {
                    throw new RuntimeException("fetch column stats failed", e);
                }
            });
        } catch (Exception e) {
            LOG.warn("fetch column stats for {}.{} failed (column stats will be skipped): {}", db, table, e.getMessage());
            result = Collections.emptyMap();
        }
        cache.putColumnStats(key, result);
        return result;
    }

    /** Whether the stats path can run at all: needs a query port and the MySQL driver. */
    private boolean isStatsAvailable() {
        for (Frontend fe : frontends) {
            if (fe.getQueryPort() != -1) {
                return true;
            }
        }
        LOG.debug("stats skipped: no fe has doris.query.port configured");
        return false;
    }

    /** Build a stable cache key from FE endpoints (LoadBalanceList has no stream()). */
    private String buildFeKey() {
        StringBuilder sb = new StringBuilder();
        for (Frontend fe : frontends) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(fe.hostHttpPortString());
        }
        return sb.toString();
    }

    /**
     * Like {@link #queryFrontends(Function)} but builds the JDBC URL with connect/socket
     * timeouts from stats options. The per-statement query timeout is applied inside
     * {@link #fetchTableStatsInternal} / {@link #fetchColumnStatsInternal} on the actual
     * PreparedStatement that runs the query.
     *
     * @throws ClassNotFoundException if the MySQL driver is not on the classpath;
     *         callers catch this via the generic {@code Exception} handler and return empty.
     */
    private <T> T queryFrontendsForStats(Function<Connection, T> function) throws Exception {
        // Fail fast on missing driver so callers can cache the empty result.
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e2) {
                throw new ClassNotFoundException("mysql jdbc driver not on classpath; stats disabled");
            }
        }
        int connectTimeoutMs;
        int socketTimeoutMs;
        try {
            connectTimeoutMs = config.getValue(DorisOptions.DORIS_STATS_JDBC_CONNECT_TIMEOUT_MS);
            socketTimeoutMs = config.getValue(DorisOptions.DORIS_STATS_JDBC_SOCKET_TIMEOUT_MS);
        } catch (OptionRequiredException e) {
            connectTimeoutMs = 3000;
            socketTimeoutMs = 5000;
        }
        Exception ex = null;
        for (Frontend frontEnd : frontends) {
            if (frontEnd.getQueryPort() == -1) {
                continue;
            }
            String url = "jdbc:mysql://" + frontEnd.getHost() + ":" + frontEnd.getQueryPort()
                    + "?connectTimeout=" + connectTimeoutMs
                    + "&socketTimeout=" + socketTimeoutMs;
            try (Connection conn = DriverManager.getConnection(url, username, password)) {
                return function.apply(conn);
            } catch (SQLException e) {
                LOG.warn("fe jdbc stats query on {} failed, err: {}", frontEnd.hostQueryPortString(), e.getMessage());
                ex = e;
            }
        }
        if (ex == null) {
            ex = new Exception("All frontends failed to execute stats query.");
        }
        throw ex;
    }

    private Optional<DorisTableStats> fetchTableStatsInternal(Connection conn, String db, String table) throws SQLException {
        String sql = "SHOW TABLE STATS " + quoteIdent(db) + "." + quoteIdent(table);
        int queryTimeoutS;
        try {
            queryTimeoutS = Math.max(1, config.getValue(DorisOptions.DORIS_STATS_JDBC_SOCKET_TIMEOUT_MS) / 1000);
        } catch (OptionRequiredException e) {
            queryTimeoutS = 5;
        }
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(queryTimeoutS);
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                int rowCountIdx = -1;
                int dataSizeIdx = -1;
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String colName = md.getColumnName(i);
                    if ("row_count".equalsIgnoreCase(colName)) {
                        rowCountIdx = i;
                    } else if ("data_size".equalsIgnoreCase(colName)) {
                        dataSizeIdx = i;
                    }
                }
                if (rowCountIdx == -1) {
                    LOG.debug("SHOW TABLE STATS returned no row_count column for {}.{}", db, table);
                    return Optional.empty();
                }
                if (rs.next()) {
                    long rowCount = rs.getLong(rowCountIdx);
                    if (rowCount < 0) {
                        return Optional.empty();
                    }
                    long dataSize = dataSizeIdx > 0 ? rs.getLong(dataSizeIdx) : -1L;
                    return Optional.of(new DorisTableStats(rowCount, dataSize));
                }
                return Optional.empty();
            }
        }
    }

    private Map<String, DorisColumnStats> fetchColumnStatsInternal(Connection conn, String db, String table,
                                                                   List<String> cols) throws SQLException {
        StringBuilder sql = new StringBuilder("SHOW COLUMN STATS ")
                .append(quoteIdent(db)).append(".").append(quoteIdent(table));
        if (cols != null && !cols.isEmpty()) {
            sql.append(" (");
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(quoteIdent(cols.get(i)));
            }
            sql.append(")");
        }
        int queryTimeoutS;
        try {
            queryTimeoutS = Math.max(1, config.getValue(DorisOptions.DORIS_STATS_JDBC_SOCKET_TIMEOUT_MS) / 1000);
        } catch (OptionRequiredException e) {
            queryTimeoutS = 5;
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            ps.setQueryTimeout(queryTimeoutS);
            try (ResultSet rs = ps.executeQuery()) {
                return parseColumnStats(rs);
            }
        }
    }

    /**
     * Parse {@code SHOW COLUMN STATS} result set, handling the multi-row-per-column case.
     *
     * <p>Doris 2.1+ may return one row per (column, index_name). When {@code index_name}
     * column exists, prefer the row whose {@code index_name} is empty/null (base index).
     * Otherwise, keep the row with the largest {@code data_size}.
     * Never sum data_size across rows.
     */
    private Map<String, DorisColumnStats> parseColumnStats(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int colNameIdx = -1;
        int ndvIdx = -1;
        int numNullsIdx = -1;
        int dataSizeIdx = -1;
        int avgSizeIdx = -1;
        int minIdx = -1;
        int maxIdx = -1;
        int indexNameIdx = -1;
        for (int i = 1; i <= md.getColumnCount(); i++) {
            String col = md.getColumnName(i);
            switch (col.toLowerCase()) {
                case "column_name": colNameIdx = i; break;
                case "ndv":         ndvIdx = i; break;
                case "num_nulls":   numNullsIdx = i; break;
                case "data_size":   dataSizeIdx = i; break;
                case "avg_size_byte": avgSizeIdx = i; break;
                case "min":         minIdx = i; break;
                case "max":         maxIdx = i; break;
                case "index_name":  indexNameIdx = i; break;
                default: break;
            }
        }
        if (colNameIdx == -1) {
            return Collections.emptyMap();
        }
        // column -> (isBaseIndex, data_size, stats)
        Map<String, DorisColumnStats> picked = new HashMap<>();
        Map<String, Long> pickedDataSize = new HashMap<>();
        Map<String, Boolean> pickedIsBase = new HashMap<>();
        while (rs.next()) {
            String colName = rs.getString(colNameIdx);
            if (colName == null) {
                continue;
            }
            long ndv = ndvIdx > 0 ? safeLong(rs, ndvIdx) : -1L;
            long numNulls = numNullsIdx > 0 ? safeLong(rs, numNullsIdx) : -1L;
            long dataSize = dataSizeIdx > 0 ? safeLong(rs, dataSizeIdx) : -1L;
            double avgSize = avgSizeIdx > 0 ? safeDouble(rs, avgSizeIdx) : -1d;
            String minLit = minIdx > 0 ? rs.getString(minIdx) : null;
            String maxLit = maxIdx > 0 ? rs.getString(maxIdx) : null;
            String indexName = indexNameIdx > 0 ? rs.getString(indexNameIdx) : null;
            boolean isBase = StringUtils.isBlank(indexName);

            Boolean prevBase = pickedIsBase.get(colName);
            if (prevBase == null) {
                // first row for this column
                picked.put(colName, new DorisColumnStats(colName, ndv, numNulls, dataSize, avgSize, minLit, maxLit));
                pickedDataSize.put(colName, dataSize);
                pickedIsBase.put(colName, isBase);
            } else {
                // prefer base index; if both same, keep larger data_size
                boolean shouldReplace = false;
                if (isBase && !prevBase) {
                    shouldReplace = true;
                } else if (isBase == prevBase && dataSize > pickedDataSize.get(colName)) {
                    shouldReplace = true;
                }
                if (shouldReplace) {
                    picked.put(colName, new DorisColumnStats(colName, ndv, numNulls, dataSize, avgSize, minLit, maxLit));
                    pickedDataSize.put(colName, dataSize);
                    pickedIsBase.put(colName, isBase);
                }
            }
        }
        return picked;
    }

    private static long safeLong(ResultSet rs, int idx) throws SQLException {
        long v = rs.getLong(idx);
        return rs.wasNull() ? -1L : v;
    }

    private static double safeDouble(ResultSet rs, int idx) throws SQLException {
        double v = rs.getDouble(idx);
        return rs.wasNull() ? -1d : v;
    }

    /** Quote a Doris identifier with backticks, escaping internal backticks. */
    private static String quoteIdent(String ident) {
        if (ident == null) {
            return "``";
        }
        return "`" + ident.replace("`", "``") + "`";
    }

    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }

}
