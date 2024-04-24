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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.common.diagnostics.StatisticReporter.REPORT_INTERVAL_MINUTES;

public class Metrics {
    private static final String JSON_API_VERSION = "1.0";

    private final ConcurrentSet<String> primaryDatabaseHashes = new ConcurrentSet<>();
    private final BaseProperties base;
    private final ServerStaticProperties serverStatic;
    private final ServerDynamicProperties serverDynamic;
    private final ConcurrentMap<String, DatabaseLoadDiagnostics> databaseLoad = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NetworkRequests> requests = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UserErrorStatistics> userErrors = new ConcurrentHashMap<>();

    public Metrics(String deploymentID, String serverID, String name, String version, boolean reportingEnabled, Path dataDirectory) {
        this.base = new BaseProperties(JSON_API_VERSION, deploymentID, serverID, name, version, reportingEnabled);
        this.serverStatic = new ServerStaticProperties();
        this.serverDynamic = new ServerDynamicProperties(version, dataDirectory);
    }

    public void takeCountsSnapshot() {
        for (String databaseHash : this.databaseLoad.keySet()) {
            this.databaseLoad.get(databaseHash).takeCountsSnapshot();
            this.requests.get(databaseHash).takeCountsSnapshot();
            this.userErrors.get(databaseHash).takeCountsSnapshot();
        }
    }

    private String hashAndAddDatabaseIfAbsent(String databaseName) {
        String databaseHash = databaseName != null ? String.valueOf(databaseName.hashCode()) : "";

        if (!databaseHash.isEmpty() && !this.databaseLoad.containsKey(databaseHash)) {
            this.databaseLoad.put(databaseHash, new DatabaseLoadDiagnostics());
        }

        if (!this.requests.containsKey(databaseHash)) {
            this.requests.put(databaseHash, new NetworkRequests());
        }

        if (!this.userErrors.containsKey(databaseHash)) {
            this.userErrors.put(databaseHash, new UserErrorStatistics());
        }

        return databaseHash;
    }

    private void updatePrimaryDatabases(String databaseHash, boolean isPrimaryServer) {
        if (isPrimaryServer) {
            primaryDatabaseHashes.add(databaseHash);
        } else {
            primaryDatabaseHashes.remove(databaseHash);
        }
    }

    public void requestSuccess(String databaseName, Metrics.NetworkRequests.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        requests.get(databaseHash).success(kind);
    }

    public void requestFail(String databaseName, Metrics.NetworkRequests.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        requests.get(databaseHash).fail(kind);
    }

    public void incrementCurrentCount(String databaseName, ConnectionPeakCounts.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoad.get(databaseHash).incrementCurrent(kind);
    }

    public void decrementCurrentCount(String databaseName, ConnectionPeakCounts.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoad.get(databaseHash).decrementCurrent(kind);
    }

    public void setCurrentCount(String databaseName, ConnectionPeakCounts.Kind kind, long value) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoad.get(databaseHash).setCurrent(kind, value);
    }

    public void registerError(String databaseName, String errorCode) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        userErrors.get(databaseHash).register(errorCode);
    }

    public void submitDatabaseDiagnostics(
            String databaseName, Metrics.DatabaseSchemaLoad schemaLoad, Metrics.DatabaseDataLoad dataLoad, boolean isPrimaryServer
    ) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        updatePrimaryDatabases(databaseHash, isPrimaryServer);
        databaseLoad.get(databaseHash).setSchemaLoad(schemaLoad);
        databaseLoad.get(databaseHash).setDataLoad(dataLoad);
    }

    protected JsonObject asJSON(boolean reporting) {
        JsonObject metrics = base.asJSON();

        if (reporting && !base.getReportingEnabled()) {
            return metrics;
        }

        serverStatic.asJSON().forEach(record -> metrics.add(record.getName(), record.getValue()));
        metrics.add("server", serverDynamic.asJSON());

        JsonArray load = new JsonArray();
        databaseLoad.keySet().forEach(databaseHash ->
                load.add(databaseLoad.get(databaseHash).asJSON(
                        databaseHash, primaryDatabaseHashes.contains(databaseHash)))
        );
        metrics.add("load", load);

        JsonArray actions = new JsonArray();
        requests.keySet().forEach(databaseHash ->
                requests.get(databaseHash).asJSON(databaseHash).forEach(actions::add));
        metrics.add("actions", actions);

        JsonArray errors = new JsonArray();
        userErrors.keySet().forEach(databaseHash ->
                userErrors.get(databaseHash).asJSON(databaseHash).forEach(errors::add));
        metrics.add("errors", errors);

        return metrics;
    }

    protected String formatPrometheus() {
        StringBuilder databaseLoadData = new StringBuilder(DatabaseLoadDiagnostics.headerPrometheus() + "\n");
        long databaseLoadDataHeaderLength = databaseLoadData.length();
        databaseLoad.keySet().forEach(databaseHash ->
                databaseLoadData.append(databaseLoad.get(databaseHash).formatPrometheus(
                        databaseHash, primaryDatabaseHashes.contains(databaseHash))));

        StringBuilder requestsDataAttempted = new StringBuilder(NetworkRequests.headerPrometheusAttempted() + "\n");
        long requestsDataAttemptedHeaderLength = requestsDataAttempted.length();
        requests.keySet().forEach(databaseHash ->
                requestsDataAttempted.append(requests.get(databaseHash).formatPrometheusAttempted(databaseHash)));

        StringBuilder requestsDataSuccessful = new StringBuilder(NetworkRequests.headerPrometheusSuccessful() + "\n");
        long requestsDataSuccessfulHeaderLength = requestsDataSuccessful.length();
        requests.keySet().forEach(databaseHash ->
                requestsDataSuccessful.append(requests.get(databaseHash).formatPrometheusSuccessful(databaseHash)));

        StringBuilder userErrorsData = new StringBuilder(UserErrorStatistics.headerPrometheus() + "\n");
        long userErrorsDataHeaderLength = userErrorsData.length();
        userErrors.keySet().forEach(databaseHash ->
                userErrorsData.append(userErrors.get(databaseHash).formatPrometheus(databaseHash)));

        return String.join(
                "\n",
                base.formatPrometheus() + serverStatic.formatPrometheus(),
                ServerDynamicProperties.headerPrometheus(), serverDynamic.formatPrometheus(),
                databaseLoadData.length() > databaseLoadDataHeaderLength ? databaseLoadData.toString() : "",
                requestsDataAttempted.length() > requestsDataAttemptedHeaderLength ? requestsDataAttempted.toString() : "",
                requestsDataSuccessful.length() > requestsDataSuccessfulHeaderLength ? requestsDataSuccessful.toString() : "",
                userErrorsData.length() > userErrorsDataHeaderLength ? userErrorsData.toString() : "");
    }

    protected String formatJSON(boolean reporting) {
        return asJSON(reporting).toString();
    }

    static class BaseProperties {
        private final String jsonApiVersion;
        private final String deploymentID;
        private final String serverID;
        private final String distribution;
        private final String distributionVersion;
        private final boolean reportingEnabled;

        BaseProperties(
                String jsonApiVersion, String deploymentID, String serverID, String distribution, String distributionVersion, boolean reportingEnabled
        ) {
            this.jsonApiVersion = jsonApiVersion;
            this.deploymentID = deploymentID;
            this.serverID = serverID;
            this.distribution = distribution;
            this.distributionVersion = distributionVersion;
            this.reportingEnabled = reportingEnabled;
        }

        JsonObject asJSON() {
            JsonObject system = new JsonObject();
            system.add("version", jsonApiVersion);
            system.add("deploymentID", deploymentID);
            system.add("serverID", serverID);
            system.add("distribution", distribution);
            system.add("timestamp", LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).toString());
            system.add("periodInSeconds", REPORT_INTERVAL_MINUTES * 60);
            system.add("enabled", reportingEnabled);
            return system;
        }

        String formatPrometheus() {
            // No deployment / server identifiers and time-based characteristics, that's for reporting only.
            return "# distribution: " + distribution + "\n" +
                    "# version: " + distributionVersion + "\n";
        }

        boolean getReportingEnabled() {
            return reportingEnabled;
        }
    }

    static class ServerStaticProperties {
        private final String os =
                System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version");

        JsonObject asJSON() {
            JsonObject system = new JsonObject();
            system.add("os", os);
            return system;
        }

        String formatPrometheus() {
            return "# os: " + os + "\n";
        }
    }

    static class ServerDynamicProperties {
        private final String version;
        private final File dbRoot;

        ServerDynamicProperties(String version, Path dataDirectory) {
            this.version = version;
            this.dbRoot = dataDirectory.toFile();
        }

        JsonObject asJSON() {
            var mxbean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long freePhysicalMemorySize = mxbean.getFreePhysicalMemorySize();
            long freeDiskSpace = dbRoot.getFreeSpace();

            JsonObject system = new JsonObject();
            system.add("version", version);
            system.add("memoryUsedInBytes", mxbean.getTotalPhysicalMemorySize() - freePhysicalMemorySize);
            system.add("memoryAvailableInBytes", freePhysicalMemorySize);
            system.add("diskUsedInBytes", dbRoot.getTotalSpace() - freeDiskSpace);
            system.add("diskAvailableInBytes", freeDiskSpace);

            return system;
        }

        static String headerPrometheus() {
            return "# TYPE server_resources_count gauge";
        }

        String formatPrometheus() {
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

        String formatPrometheus(String database) {
            return "typedb_schema_data_count{database=\"" + database + "\", kind=\"typeCount\"} " + typeCount;
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

        String formatPrometheus(String database) {
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

        public void takeCountsSnapshot() {
            peakCounts.replaceAll((kind, value) -> new AtomicLong(counts.get(kind).get()));
        }

        public void incrementCurrent(Kind kind) {
            long value = counts.get(kind).incrementAndGet();
            updatePeakValue(kind, value);
        }

        public void decrementCurrent(Kind kind) {
            counts.get(kind).decrementAndGet();
        }

        public void setCurrent(Kind kind, long value) {
            counts.get(kind).set(value);
            updatePeakValue(kind, value);
        }

        private void updatePeakValue(Kind kind, long value) {
            if (peakCounts.get(kind).get() < value) {
                peakCounts.get(kind).set(value);
            }
        }

        JsonObject asJSON() {
            JsonObject peak = new JsonObject();
            for (Kind kind : Kind.values()) {
                if (kind == Kind.UNKNOWN) {
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
        private ConnectionPeakCounts connectionPeakCounts;

        DatabaseLoadDiagnostics() {
            this.schemaLoad = new DatabaseSchemaLoad();
            this.dataLoad = new DatabaseDataLoad();
            this.connectionPeakCounts = new ConnectionPeakCounts();
        }

        public void setSchemaLoad(Metrics.DatabaseSchemaLoad schemaLoad) {
            this.schemaLoad = schemaLoad;
        }

        public void setDataLoad(Metrics.DatabaseDataLoad dataLoad) {
            this.dataLoad = dataLoad;
        }

        public void incrementCurrent(ConnectionPeakCounts.Kind kind) {
            connectionPeakCounts.incrementCurrent(kind);
        }

        public void decrementCurrent(ConnectionPeakCounts.Kind kind) {
            connectionPeakCounts.decrementCurrent(kind);
        }

        public void setCurrent(ConnectionPeakCounts.Kind kind, long value) {
            connectionPeakCounts.setCurrent(kind, value);
        }

        public void takeCountsSnapshot() {
            connectionPeakCounts.takeCountsSnapshot();
        }

        JsonObject asJSON(String database, boolean isPrimaryServer) {
            JsonObject load = new JsonObject();
            load.add("database", database);
            if (isPrimaryServer) {
                load.add("schema", schemaLoad.asJSON());
                load.add("data", dataLoad.asJSON());
            } else { // TODO: Remove after tests
                System.out.println("Could send some data, but I am not a Primary server! " + schemaLoad.asJSON() + dataLoad.asJSON());
            }
            load.add("connection", connectionPeakCounts.asJSON());
            return load;
        }
        
        static String headerPrometheus() {
            return "# TYPE typedb_schema_data_count gauge";
        }

        String formatPrometheus(String database, boolean isPrimaryServer) {
            if (!isPrimaryServer) {
                return "";
            }
            return schemaLoad.formatPrometheus(database) + "\n" + dataLoad.formatPrometheus(database);
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

        private final ConcurrentMap<Kind, AtomicLong> successful = new ConcurrentHashMap<>();
        private final ConcurrentMap<Kind, AtomicLong> failed = new ConcurrentHashMap<>();

        private ConcurrentMap<Kind, AtomicLong> successfulSnapshot = new ConcurrentHashMap<>();
        private ConcurrentMap<Kind, AtomicLong> failedSnapshot = new ConcurrentHashMap<>();

        NetworkRequests() {
            for (var kind : Kind.values()) {
                successful.put(kind, new AtomicLong(0));
                failed.put(kind, new AtomicLong(0));
            }

            takeCountsSnapshot();
        }

        public void takeCountsSnapshot() {
            successfulSnapshot = successful;
            failedSnapshot = failed;
        }

        public void success(Kind kind) {
            successful.get(kind).incrementAndGet();
        }

        public void fail(Kind kind) {
            failed.get(kind).incrementAndGet();
        }

        JsonArray asJSON(String database) {
            JsonArray requests = new JsonArray();

            for (var kind : Kind.values()) {
                long successfulValue = successful.get(kind).get() - successfulSnapshot.get(kind).get();
                long failedValue = failed.get(kind).get() - failedSnapshot.get(kind).get();
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

        static String headerPrometheusAttempted() {
            return "# TYPE typedb_attempted_requests_total counter";
        }

        static String headerPrometheusSuccessful() {
            return "# TYPE typedb_successful_requests_total counter";
        }

        String formatPrometheusAttempted(String database) {
            StringBuilder buf = new StringBuilder();

            for (var kind : Kind.values()) {
                var attempted = successful.get(kind).get() + failed.get(kind).get();
                buf.append("typedb_attempted_requests_total{");
                if (!database.isEmpty()) {
                    buf.append("database=\"").append(database).append("\", ");
                }
                buf.append("kind=\"").append(kind).append("\"} ").append(attempted).append("\n");
            }

            return buf.toString();
        }

        String formatPrometheusSuccessful(String database) {
            StringBuilder buf = new StringBuilder();

            for (var kind : Kind.values()) {
                buf.append("typedb_successful_requests_total{");
                if (!database.isEmpty()) {
                    buf.append("database=\"").append(database).append("\", ");
                }
                buf.append("kind=\"").append(kind).append("\"} ").append(successful.get(kind)).append("\n");
            }

            return buf.toString();
        }
    }

    private static class UserErrorStatistics {
        private final ConcurrentMap<String, AtomicLong> errorCounts = new ConcurrentHashMap<>();
        private ConcurrentMap<String, AtomicLong> errorCountsSnapshot = new ConcurrentHashMap<>();

        public void takeCountsSnapshot() {
            errorCountsSnapshot = errorCounts;
        }

        public void register(String errorCode) {
            errorCounts.computeIfAbsent(errorCode, c -> new AtomicLong(0)).incrementAndGet();
        }

        JsonArray asJSON(String database) {
            JsonArray errors = new JsonArray();

            for (String code : errorCounts.keySet()) {
                JsonObject errorObject = new JsonObject();
                errorObject.add("code", code);
                if (!database.isEmpty()) {
                    errorObject.add("database", database);
                }
                long countDiff = errorCounts.get(code).get() - errorCountsSnapshot.getOrDefault(code, new AtomicLong(0)).get();
                errorObject.add("count", countDiff);
                errors.add(errorObject);
            }

            return errors;
        }

        static String headerPrometheus() {
            return "# TYPE typedb_error_total counter";
        }

        String formatPrometheus(String database) {
            if (errorCounts.isEmpty()) {
                return "";
            }

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
