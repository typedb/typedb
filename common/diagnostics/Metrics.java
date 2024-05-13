/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.parameters.Arguments;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.common.diagnostics.StatisticReporter.REPORT_INTERVAL_MINUTES;
import static com.vaticle.typedb.core.common.diagnostics.Version.DIAGNOSTICS_JSON_SCHEMA_VERSION;

public class Metrics {
    private final ConcurrentSet<String> primaryDatabaseHashes = new ConcurrentSet<>();
    private final BaseProperties base;
    private final ServerProperties serverProperties;
    private final ConcurrentMap<String, DatabaseLoadDiagnostics> databaseLoad = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NetworkRequests> requests = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UserErrorStatistics> userErrors = new ConcurrentHashMap<>();

    public Metrics(String deploymentID, String serverID, String name, String version, boolean reportingEnabled, Path dataDirectory) {
        this.base = new BaseProperties(DIAGNOSTICS_JSON_SCHEMA_VERSION, deploymentID, serverID, name, reportingEnabled);
        this.serverProperties = new ServerProperties(version, dataDirectory);
    }

    public void takeSnapshot() {
        this.base.updateSinceTimestamp();
        this.databaseLoad.values().forEach(DatabaseLoadDiagnostics::takeSnapshot);
        this.requests.values().forEach(NetworkRequests::takeSnapshot);
        this.userErrors.values().forEach(UserErrorStatistics::takeSnapshot);
    }

    private String hashAndAddDatabaseIfAbsent(@Nullable String databaseName) {
        String databaseHash = databaseName != null ? String.valueOf(databaseName.hashCode()) : "";

        if (!databaseHash.isEmpty()) {
            this.databaseLoad.computeIfAbsent(databaseHash, val -> new DatabaseLoadDiagnostics());
        }

        this.requests.computeIfAbsent(databaseHash, val -> new NetworkRequests());
        this.userErrors.computeIfAbsent(databaseHash, val -> new UserErrorStatistics());

        return databaseHash;
    }

    private void updatePrimaryDatabases(String databaseHash, boolean isPrimaryServer) {
        if (isPrimaryServer) {
            primaryDatabaseHashes.add(databaseHash);
        } else {
            primaryDatabaseHashes.remove(databaseHash);
        }
    }

    public void requestSuccess(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        requests.get(databaseHash).success(kind);
    }

    public void requestFail(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        requests.get(databaseHash).fail(kind);
    }

    public void incrementCurrentCount(@Nullable String databaseName, ConnectionPeakCounts.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoad.get(databaseHash).incrementCurrent(kind);
    }

    public void decrementCurrentCount(@Nullable String databaseName, ConnectionPeakCounts.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoad.get(databaseHash).decrementCurrent(kind);
    }

    public void registerError(@Nullable String databaseName, String errorCode) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        userErrors.get(databaseHash).register(errorCode);
    }

    public void submitDatabaseDiagnostics(Set<DatabaseDiagnostics> databaseDiagnostics) {
        Set<String> deletedDatabases = new HashSet<>(databaseLoad.keySet());

        for (DatabaseDiagnostics diagnostics : databaseDiagnostics) {
            String databaseHash = hashAndAddDatabaseIfAbsent(diagnostics.databaseName());
            databaseLoad.get(databaseHash).setSchemaLoad(diagnostics.schemaLoad());
            databaseLoad.get(databaseHash).setDataLoad(diagnostics.dataLoad());
            updatePrimaryDatabases(databaseHash, diagnostics.isPrimaryServer());
            deletedDatabases.remove(databaseHash);
        }

        deletedDatabases.forEach(databaseHash -> databaseLoad.get(databaseHash).setDatabaseDeleted());
    }
    
    private static String formatDateTime(LocalDateTime dateTime) {
        return dateTime.truncatedTo(ChronoUnit.SECONDS).toString();
    }

    protected JsonObject asJSON(boolean reporting) {
        boolean addUsageDiagnostics = reporting && !base.getReportingEnabled();

        JsonObject metrics = base.asJSON();
        metrics.add("server", serverProperties.asJSON(!addUsageDiagnostics));

        if (!addUsageDiagnostics) {
            return metrics;
        }

        JsonArray load = new JsonArray();
        databaseLoad.forEach((databaseHash, diagnostics) ->
                diagnostics.asJSON(databaseHash, primaryDatabaseHashes.contains(databaseHash)).forEach(load::add));
        metrics.add("load", load);

        JsonArray actions = new JsonArray();
        requests.forEach((databaseHash, diagnostics) -> diagnostics.asJSON(databaseHash).forEach(actions::add));
        metrics.add("actions", actions);

        JsonArray errors = new JsonArray();
        userErrors.forEach((databaseHash, diagnostics) -> diagnostics.asJSON(databaseHash).forEach(errors::add));
        metrics.add("errors", errors);

        return metrics;
    }

    protected String formatPrometheus() {
        StringBuilder databaseLoadData = new StringBuilder(DatabaseLoadDiagnostics.prometheusHeader() + "\n");
        long databaseLoadDataHeaderLength = databaseLoadData.length();
        databaseLoad.keySet().forEach(databaseHash ->
                databaseLoadData.append(databaseLoad.get(databaseHash).prometheusDiagnostics(
                        databaseHash, primaryDatabaseHashes.contains(databaseHash))));

        StringBuilder requestsDataAttempted = new StringBuilder(NetworkRequests.prometheusHeaderAttempted() + "\n");
        long requestsDataAttemptedHeaderLength = requestsDataAttempted.length();
        requests.keySet().forEach(databaseHash ->
                requestsDataAttempted.append(requests.get(databaseHash).prometheusDiagnosticsAttempted(databaseHash)));

        StringBuilder requestsDataSuccessful = new StringBuilder(NetworkRequests.prometheusHeaderSuccessful() + "\n");
        long requestsDataSuccessfulHeaderLength = requestsDataSuccessful.length();
        requests.keySet().forEach(databaseHash ->
                requestsDataSuccessful.append(requests.get(databaseHash).prometheusDiagnosticsSuccessful(databaseHash)));

        StringBuilder userErrorsData = new StringBuilder(UserErrorStatistics.prometheusHeader() + "\n");
        long userErrorsDataHeaderLength = userErrorsData.length();
        userErrors.keySet().forEach(databaseHash ->
                userErrorsData.append(userErrors.get(databaseHash).prometheusDiagnostics(databaseHash)));

        return String.join(
                "\n",
                base.prometheusCommentData() + serverProperties.prometheusCommentData(),
                ServerProperties.prometheusHeader(), serverProperties.prometheusDiagnostics(),
                databaseLoadData.length() > databaseLoadDataHeaderLength ? databaseLoadData.toString() : "",
                requestsDataAttempted.length() > requestsDataAttemptedHeaderLength ? requestsDataAttempted.toString() : "",
                requestsDataSuccessful.length() > requestsDataSuccessfulHeaderLength ? requestsDataSuccessful.toString() : "",
                userErrorsData.length() > userErrorsDataHeaderLength ? userErrorsData.toString() : "");
    }

    protected String formatJSON(boolean reporting) {
        return asJSON(reporting).toString();
    }

    public static class DatabaseDiagnostics {
        private String databaseName;
        private DatabaseSchemaLoad schemaLoad;
        private DatabaseDataLoad dataLoad;
        private boolean isPrimaryServer;

        public DatabaseDiagnostics(
                String databaseName, DatabaseSchemaLoad schemaLoad, DatabaseDataLoad dataLoad, boolean isPrimaryServer
        ) {
            this.databaseName = databaseName;
            this.schemaLoad = schemaLoad;
            this.dataLoad = dataLoad;
            this.isPrimaryServer = isPrimaryServer;
        }

        public String databaseName() {
            return databaseName;
        }

        public DatabaseSchemaLoad schemaLoad() {
            return schemaLoad;
        }

        public DatabaseDataLoad dataLoad() {
            return dataLoad;
        }

        public boolean isPrimaryServer() {
            return isPrimaryServer;
        }
    }

    static class BaseProperties {
        private final int jsonApiVersion;
        private final String deploymentID;
        private final String serverID;
        private final String distribution;
        private LocalDateTime sinceTimestamp;
        private final boolean reportingEnabled;

        BaseProperties(
                int jsonApiVersion, String deploymentID, String serverID, String distribution, boolean reportingEnabled
        ) {
            this.jsonApiVersion = jsonApiVersion;
            this.deploymentID = deploymentID;
            this.serverID = serverID;
            this.distribution = distribution;
            this.reportingEnabled = reportingEnabled;
            updateSinceTimestamp();
        }

        void updateSinceTimestamp() {
            this.sinceTimestamp = LocalDateTime.now(ZoneOffset.UTC);
        }

        JsonObject asJSON() {
            JsonObject system = new JsonObject();
            system.add("version", jsonApiVersion);
            system.add("deploymentID", deploymentID);
            system.add("serverID", serverID);
            system.add("distribution", distribution);
            system.add("timestamp", formatDateTime(LocalDateTime.now(ZoneOffset.UTC)));
            system.add("sinceTimestamp", formatDateTime(sinceTimestamp));
            system.add("periodInSeconds", REPORT_INTERVAL_MINUTES * 60);
            system.add("enabled", reportingEnabled);
            return system;
        }

        String prometheusCommentData() {
            // No deployment / server identifiers and time-based characteristics, that's for reporting only.
            return "# distribution: " + distribution + "\n";
        }

        boolean getReportingEnabled() {
            return reportingEnabled;
        }
    }

    static class ServerProperties {
        private final LocalDateTime startTime;
        private final String osName;
        private final String osArch;
        private final String osVersion;
        private final String version;
        private final File dbRoot;

        ServerProperties(String version, Path dataDirectory) {
            this.startTime = LocalDateTime.now(ZoneOffset.UTC);
            this.osName = System.getProperty("os.name");
            this.osArch = System.getProperty("os.arch");
            this.osVersion = System.getProperty("os.version");
            this.version = version;
            this.dbRoot = dataDirectory.toFile();
        }

        private long getUptimeInSeconds() {
            return Duration.between(startTime, LocalDateTime.now(ZoneOffset.UTC)).getSeconds();
        }

        JsonObject asJSON(boolean isNoUsageDiagnostics) {
            var mxbean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long freePhysicalMemorySize = mxbean.getFreePhysicalMemorySize();
            long freeDiskSpace = dbRoot.getFreeSpace();

            JsonObject server = new JsonObject();
            server.add("version", version);

            if (isNoUsageDiagnostics) {
                return server;
            }

            server.add("uptimeInSeconds", getUptimeInSeconds());

            JsonObject os = new JsonObject();
            os.add("name", osName);
            os.add("arch", osArch);
            os.add("version", osVersion);
            server.add("os", os);

            server.add("memoryUsedInBytes", mxbean.getTotalPhysicalMemorySize() - freePhysicalMemorySize);
            server.add("memoryAvailableInBytes", freePhysicalMemorySize);
            server.add("diskUsedInBytes", dbRoot.getTotalSpace() - freeDiskSpace);
            server.add("diskAvailableInBytes", freeDiskSpace);

            return server;
        }

        static String prometheusHeader() {
            return "# TYPE server_resources_count gauge";
        }

        String prometheusCommentData() {
            return "# version: " + version + "\n" +
                    "# os: " + osName + " " + osArch + " " + osVersion + "\n";
        }

        String prometheusDiagnostics() {
            var mxbean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long freePhysicalMemorySize = mxbean.getFreePhysicalMemorySize();
            long freeDiskSpace = dbRoot.getFreeSpace();

            String header = "server_resources_count{kind=";

            StringBuilder buf = new StringBuilder();
            buf.append(header).append("\"memoryUsedInBytes\"} ").append(mxbean.getTotalPhysicalMemorySize() - freePhysicalMemorySize).append("\n")
                    .append(header).append("\"memoryAvailableInBytes\"} ").append(freePhysicalMemorySize).append("\n")
                    .append(header).append("\"diskUsedInBytes\"} ").append(dbRoot.getTotalSpace() - freeDiskSpace).append("\n")
                    .append(header).append("\"diskAvailableInBytes\"} ").append(freeDiskSpace).append("\n");

            return buf.toString();
        }
    }

    public static class DatabaseSchemaLoad {
        public long typeCount = 0;

        public DatabaseSchemaLoad() {
        }

        public DatabaseSchemaLoad(long typeCount) {
            this.typeCount = typeCount;
        }

        JsonObject asJSON() {
            JsonObject schema = new JsonObject();
            schema.add("typeCount", typeCount);
            return schema;
        }

        String prometheusDiagnostics(String database) {
            return "typedb_schema_data_count{database=\"" + database + "\", kind=\"typeCount\"} " + typeCount + "\n";
        }
    }

    public static class DatabaseDataLoad {
        public long entityCount = 0;
        public long relationCount = 0;
        public long attributeCount = 0;
        public long hasCount = 0;
        public long roleCount = 0;
        public long storageInBytes = 0;
        public long storageKeyCount = 0;

        public DatabaseDataLoad() {
        }

        public DatabaseDataLoad(
                long entityCount,
                long relationCount,
                long attributeCount,
                long hasCount,
                long roleCount,
                long storageInBytes,
                long storageKeyCount
        ) {
            this.entityCount = entityCount;
            this.relationCount = relationCount;
            this.attributeCount = attributeCount;
            this.hasCount = hasCount;
            this.roleCount = roleCount;
            this.storageInBytes = storageInBytes;
            this.storageKeyCount = storageKeyCount;
        }

        JsonObject asJSON() {
            JsonObject data = new JsonObject();
            data.add("entityCount", entityCount);
            data.add("relationCount", relationCount);
            data.add("attributeCount", attributeCount);
            data.add("hasCount", hasCount);
            data.add("roleCount", roleCount);
            data.add("storageInBytes", storageInBytes);
            data.add("storageKeyCount", storageKeyCount);
            return data;
        }

        String prometheusDiagnostics(String database) {
            String header = "typedb_schema_data_count{database=\"" + database + "\", kind=";

            StringBuilder buf = new StringBuilder();
            buf.append(header).append("\"entityCount\"} ").append(entityCount).append("\n")
                    .append(header).append("\"relationCount\"} ").append(relationCount).append("\n")
                    .append(header).append("\"attributeCount\"} ").append(attributeCount).append("\n")
                    .append(header).append("\"hasCount\"} ").append(hasCount).append("\n")
                    .append(header).append("\"roleCount\"} ").append(roleCount).append("\n")
                    .append(header).append("\"storageInBytes\"} ").append(storageInBytes).append("\n")
                    .append(header).append("\"storageKeyCount\"} ").append(storageKeyCount).append("\n");

            return buf.toString();
        }
    }

    public static class ConnectionPeakCounts {
        public enum Kind {
            SCHEMA_TRANSACTIONS("schemaTransactionPeakCount"),
            READ_TRANSACTIONS("readTransactionPeakCount"),
            WRITE_TRANSACTIONS("writeTransactionPeakCount"),
            UNKNOWN("unknown");

            private final String jsonName;

            Kind(String jsonName) {
                this.jsonName = jsonName;
            }

            public String getJsonName() {
                return jsonName;
            }

            public static Kind getKind(Arguments.Session.Type sessionType, Arguments.Transaction.Type transactionType) {
                if (sessionType == Arguments.Session.Type.SCHEMA) {
                    return SCHEMA_TRANSACTIONS;
                }

                switch (transactionType) {
                    case READ:
                        return READ_TRANSACTIONS;
                    case WRITE:
                        return WRITE_TRANSACTIONS;
                }

                return UNKNOWN; // We don't want to throw from the Diagnostics service.
            }
        }

        private final ConcurrentMap<Kind, AtomicLong> counts = new ConcurrentHashMap<>();
        private final ConcurrentMap<Kind, AtomicLong> peakCounts = new ConcurrentHashMap<>();

        ConnectionPeakCounts() {
            for (Kind kind : Kind.values()) {
                counts.put(kind, new AtomicLong(0));
                peakCounts.put(kind, new AtomicLong(0));
            }
        }

        public synchronized void takeSnapshot() {
            peakCounts.replaceAll((kind, value) -> new AtomicLong(counts.get(kind).get()));
        }

        public void incrementCurrent(Kind kind) {
            long value = counts.get(kind).incrementAndGet();
            updatePeakValue(kind, value);
        }

        public void decrementCurrent(Kind kind) {
            counts.get(kind).decrementAndGet();
        }

        private synchronized void updatePeakValue(Kind kind, long value) {
            if (peakCounts.get(kind).get() < value) {
                peakCounts.get(kind).set(value);
            }
        }

        boolean isEmpty() {
            return peakCounts.values().stream().mapToLong(AtomicLong::get).sum() == 0;
        }

        JsonObject asJSON() {
            JsonObject peak = new JsonObject();
            for (Kind kind : Kind.values()) {
                if (kind == Kind.UNKNOWN) {
                    assert (peakCounts.get(kind).get() == 0);
                    continue;
                }
                peak.add(kind.getJsonName(), peakCounts.get(kind).get());
            }
            return peak;
        }
    }

    static class DatabaseLoadDiagnostics {
        private DatabaseSchemaLoad schemaLoad;
        private DatabaseDataLoad dataLoad;
        private final ConnectionPeakCounts connectionPeakCounts;
        private boolean isDeleted;

        DatabaseLoadDiagnostics() {
            this.schemaLoad = new DatabaseSchemaLoad();
            this.dataLoad = new DatabaseDataLoad();
            this.connectionPeakCounts = new ConnectionPeakCounts();
            this.isDeleted = false;
        }

        public void setSchemaLoad(DatabaseSchemaLoad schemaLoad) {
            isDeleted = false;
            this.schemaLoad = schemaLoad;
        }

        public void setDataLoad(DatabaseDataLoad dataLoad) {
            isDeleted = false;
            this.dataLoad = dataLoad;
        }

        public void setDatabaseDeleted() {
            isDeleted = true;
            this.schemaLoad = new DatabaseSchemaLoad();
            this.dataLoad = new DatabaseDataLoad();
        }

        public void incrementCurrent(ConnectionPeakCounts.Kind kind) {
            connectionPeakCounts.incrementCurrent(kind);
        }

        public void decrementCurrent(ConnectionPeakCounts.Kind kind) {
            connectionPeakCounts.decrementCurrent(kind);
        }

        public void takeSnapshot() {
            connectionPeakCounts.takeSnapshot();
        }

        JsonArray asJSON(String database, boolean isPrimaryServer) {
            JsonArray load = new JsonArray();

            if (!isDeleted || !connectionPeakCounts.isEmpty()) {
                JsonObject loadObject = new JsonObject();

                loadObject.add("database", database);
                if (isPrimaryServer) {
                    loadObject.add("schema", schemaLoad.asJSON());
                    loadObject.add("data", dataLoad.asJSON());
                }
                loadObject.add("connection", connectionPeakCounts.asJSON());

                load.add(loadObject);
            }

            return load;
        }

        static String prometheusHeader() {
            return "# TYPE typedb_schema_data_count gauge";
        }

        String prometheusDiagnostics(String database, boolean isPrimaryServer) {
            if (!isPrimaryServer || isDeleted) {
                return "";
            }
            return schemaLoad.prometheusDiagnostics(database) + dataLoad.prometheusDiagnostics(database);
        }
    }

    public static class NetworkRequests {
        public enum Kind {
            CONNECTION_OPEN,
            SERVERS_ALL,
            USERS_CONTAINS,
            USERS_CREATE,
            USERS_DELETE,
            USERS_ALL,
            USERS_GET,
            USERS_PASSWORD_SET,
            USER_PASSWORD_UPDATE,
            USER_TOKEN,
            DATABASES_CONTAINS,
            DATABASES_CREATE,
            DATABASES_GET,
            DATABASES_ALL,
            DATABASE_SCHEMA,
            DATABASE_TYPE_SCHEMA,
            DATABASE_RULE_SCHEMA,
            DATABASE_DELETE,
            SESSION_OPEN,
            SESSION_CLOSE,
            TRANSACTION_EXECUTE,
        }

        class RequestInfo implements Cloneable {
            private final AtomicLong successful;
            private final AtomicLong failed;

            RequestInfo() {
                successful = new AtomicLong(0);
                failed = new AtomicLong(0);
            }

            public void success() {
                successful.incrementAndGet();
            }

            public void fail() {
                failed.incrementAndGet();
            }

            public long getSuccessful() {
                return successful.get();
            }

            public long getFailed() {
                return failed.get();
            }

            public long getAttempted() {
                return successful.get() + failed.get();
            }

            @Override
            public RequestInfo clone() {
                RequestInfo clone = new RequestInfo();
                clone.successful.getAndAdd(successful.get());
                clone.failed.getAndAdd(failed.get());
                return clone;
            }
        }

        private final ConcurrentMap<Kind, RequestInfo> requestInfos = new ConcurrentHashMap<>();
        private final ConcurrentMap<Kind, RequestInfo> requestInfosSnapshot = new ConcurrentHashMap<>();

        public synchronized void takeSnapshot() {
            requestInfosSnapshot.clear();
            requestInfos.forEach((kind, requestInfo) -> requestInfosSnapshot.put(kind, requestInfo.clone()));
        }

        public void success(Kind kind) {
            requestInfos.computeIfAbsent(kind, val -> new RequestInfo()).success();
        }

        public void fail(Kind kind) {
            requestInfos.computeIfAbsent(kind, val -> new RequestInfo()).fail();
        }

        private synchronized long getSuccessfulDelta(Kind kind) {
            return requestInfos.getOrDefault(kind, new RequestInfo()).getSuccessful() -
                    requestInfosSnapshot.getOrDefault(kind, new RequestInfo()).getSuccessful();
        }

        private synchronized long getFailedDelta(Kind kind) {
            return requestInfos.getOrDefault(kind, new RequestInfo()).getFailed() -
                    requestInfosSnapshot.getOrDefault(kind, new RequestInfo()).getFailed();
        }

        JsonArray asJSON(String database) {
            JsonArray requests = new JsonArray();

            for (var kind : requestInfos.keySet()) {
                long successfulValue = getSuccessfulDelta(kind);
                long failedValue = getFailedDelta(kind);
                if (successfulValue == 0 && failedValue == 0) {
                    continue;
                }

                JsonObject requestObject = new JsonObject();
                requestObject.add("name", kind.name());
                if (!database.isEmpty()) {
                    requestObject.add("database", database);
                }
                requestObject.add("successful", successfulValue);
                requestObject.add("failed", failedValue);
                requests.add(requestObject);
            }

            return requests;
        }

        static String prometheusHeaderAttempted() {
            return "# TYPE typedb_attempted_requests_total counter";
        }

        static String prometheusHeaderSuccessful() {
            return "# TYPE typedb_successful_requests_total counter";
        }

        String prometheusDiagnosticsAttempted(String database) {
            StringBuilder buf = new StringBuilder();

            for (var kind : requestInfos.keySet()) {
                buf.append("typedb_attempted_requests_total{");
                if (!database.isEmpty()) {
                    buf.append("database=\"").append(database).append("\", ");
                }
                buf.append("kind=\"").append(kind).append("\"} ").append(requestInfos.get(kind).getAttempted()).append("\n");
            }

            return buf.toString();
        }

        String prometheusDiagnosticsSuccessful(String database) {
            StringBuilder buf = new StringBuilder();

            for (var kind : requestInfos.keySet()) {
                buf.append("typedb_successful_requests_total{");
                if (!database.isEmpty()) {
                    buf.append("database=\"").append(database).append("\", ");
                }
                buf.append("kind=\"").append(kind).append("\"} ").append(requestInfos.get(kind).getSuccessful()).append("\n");
            }

            return buf.toString();
        }
    }

    private static class UserErrorStatistics {
        private final ConcurrentMap<String, AtomicLong> errorCounts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> errorCountsSnapshot = new ConcurrentHashMap<>();

        public synchronized void takeSnapshot() {
            errorCountsSnapshot.clear();
            errorCounts.forEach((code, count) -> errorCountsSnapshot.put(code, new AtomicLong(count.get())));
        }

        public synchronized long getCountDelta(String code ) {
            return errorCounts.getOrDefault(code, new AtomicLong(0)).get() -
                    errorCountsSnapshot.getOrDefault(code, new AtomicLong(0)).get();
        }

        public void register(String errorCode) {
            errorCounts.computeIfAbsent(errorCode, count -> new AtomicLong(0)).incrementAndGet();
        }

        JsonArray asJSON(String database) {
            JsonArray errors = new JsonArray();

            for (String code : errorCounts.keySet()) {
                long countDelta = getCountDelta(code);
                if (countDelta == 0) {
                    continue;
                }

                JsonObject errorObject = new JsonObject();
                errorObject.add("code", code);
                if (!database.isEmpty()) {
                    errorObject.add("database", database);
                }
                errorObject.add("count", countDelta);
                errors.add(errorObject);
            }

            return errors;
        }

        static String prometheusHeader() {
            return "# TYPE typedb_error_total counter";
        }

        String prometheusDiagnostics(String database) {
            StringBuilder buf = new StringBuilder();

            for (String code : errorCounts.keySet()) {
                long count = errorCounts.get(code).get();
                buf.append("typedb_error_total{");
                if (!database.isEmpty()) {
                    buf.append("database=\"").append(database).append("\", ");
                }
                buf.append("code=\"").append(code).append("\"} ").append(count).append("\n");
            }

            return buf.toString();
        }
    }
}
