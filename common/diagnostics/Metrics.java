/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.vaticle.typedb.core.common.parameters.Arguments;

import javax.annotation.Nullable;
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
    private final BaseProperties base;
    private final ServerStaticProperties serverStatic;
    private final ServerDynamicProperties serverDynamic;
    private final ConcurrentMap<String, DatabaseLoadDiagnostics> databaseLoadDiagnostics = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NetworkRequests> requests = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UserErrorStatistics> userErrors = new ConcurrentHashMap<>();

    public Metrics(String deploymentID, String serverID, String name, String version, boolean reportingEnabled, Path dataDirectory) {
        this.base = new BaseProperties(JSON_API_VERSION, deploymentID, serverID, name, reportingEnabled);
        this.serverStatic = new ServerStaticProperties();
        this.serverDynamic = new ServerDynamicProperties(version, dataDirectory);
    }

    public void takeCountsSnapshot() {
        for (String databaseHash : this.databaseLoadDiagnostics.keySet()) {
            this.databaseLoadDiagnostics.get(databaseHash).takeCountsSnapshot();
            this.requests.get(databaseHash).takeCountsSnapshot();
            this.userErrors.get(databaseHash).takeCountsSnapshot();
        }
    }

    public String hashAndAddDatabaseIfAbsent(@Nullable String databaseName) {
        String databaseHash = databaseName != null ? String.valueOf(databaseName.hashCode()) : "";

        if (!databaseHash.isEmpty() && !this.databaseLoadDiagnostics.containsKey(databaseHash)) {
            this.databaseLoadDiagnostics.put(databaseHash, new DatabaseLoadDiagnostics());
        }

        if (!this.requests.containsKey(databaseHash)) {
            this.requests.put(databaseHash, new NetworkRequests());
        }

        if (!this.userErrors.containsKey(databaseHash)) {
            this.userErrors.put(databaseHash, new UserErrorStatistics());
        }

        return databaseHash;
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
        databaseLoadDiagnostics.get(databaseHash).incrementCurrent(kind);
    }

    public void decrementCurrentCount(@Nullable String databaseName, ConnectionPeakCounts.Kind kind) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoadDiagnostics.get(databaseHash).decrementCurrent(kind);
    }

    public void setCurrentCount(@Nullable String databaseName, ConnectionPeakCounts.Kind kind, long value) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoadDiagnostics.get(databaseHash).setCurrent(kind, value);
    }

    public void registerError(@Nullable String databaseName, String errorCode) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        userErrors.get(databaseHash).register(errorCode);
    }

    public void submitDatabaseDiagnostics(
            String databaseName, Metrics.DatabaseSchemaLoad schemaLoad, Metrics.DatabaseDataLoad dataLoad) {
        String databaseHash = hashAndAddDatabaseIfAbsent(databaseName);
        databaseLoadDiagnostics.get(databaseHash).setSchemaLoad(schemaLoad);
        databaseLoadDiagnostics.get(databaseHash).setDataLoad(dataLoad);
    }

    protected JsonObject asJSON(boolean reporting) { // TODO: Only leader sends some stats about specific databases?
        JsonObject metrics = base.asJSON();

        if (reporting && !base.getReportingEnabled()) {
            return metrics;
        }

        for (var record : serverStatic.asJSON()) {
            metrics.add(record.getName(), record.getValue());
        }

        metrics.add("server", serverDynamic.asJSON());

        JsonArray load = new JsonArray();
        databaseLoadDiagnostics.keySet().forEach(databaseHash ->
            load.add(databaseLoadDiagnostics.get(databaseHash).asJSON(databaseHash))
        );
        metrics.add("load", load);

        JsonArray actions = new JsonArray();
        requests.keySet().forEach(databaseHash -> {
            String jsonDatabase = !databaseHash.isEmpty() ? databaseHash : null;
            requests.get(databaseHash).asJSON(jsonDatabase).forEach(actions::add);
        });
        metrics.add("actions", actions);

        JsonArray errors = new JsonArray();
        userErrors.keySet().forEach(databaseHash -> {
            String jsonDatabase = !databaseHash.isEmpty() ? databaseHash : null;
            userErrors.get(databaseHash).asJSON(jsonDatabase).forEach(errors::add);
        });
        metrics.add("errors", errors);

        return metrics;
    }

    protected String formatPrometheus() {
        String data = base.formatPrometheus();
        data += serverStatic.formatPrometheus();
        data += serverDynamic.formatPrometheus();
        for (var databaseName : requests.keySet()) {
            data = String.join(
                    "\n",
                    data,
                    requests.get(databaseName).formatPrometheus(databaseName),
                    userErrors.get(databaseName).formatPrometheus(databaseName));
        }
        return data;
    }

    protected String formatJSON(boolean reporting) {
        return asJSON(reporting).toString();
    }

    static class BaseProperties {
        private final String jsonApiVersion;
        private final String deploymentID;
        private final String serverID;
        private final String distribution;
        private final boolean reportingEnabled;

        BaseProperties(String jsonApiVersion, String deploymentID, String serverID, String distribution, boolean reportingEnabled) {
            this.jsonApiVersion = jsonApiVersion;
            this.deploymentID = deploymentID;
            this.serverID = serverID;
            this.distribution = distribution;
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
            return "# distribution: " + distribution + "\n";
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

        String formatPrometheus() {
            var mxbean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long freePhysicalMemorySize = mxbean.getFreePhysicalMemorySize();
            long freeDiskSpace = dbRoot.getFreeSpace();

            return "# version: " + version + "\n" +
                    "# memoryUsedInBytes: " + (mxbean.getTotalPhysicalMemorySize() - freePhysicalMemorySize) + "\n" +
                    "# memoryAvailableInBytes: " + freePhysicalMemorySize + "\n" +
                    "# diskUsedInBytes: " + (dbRoot.getTotalSpace() - freeDiskSpace) + "\n" +
                    "# diskAvailableInBytes: " + freeDiskSpace + "\n";
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

        String formatPrometheus() {
            return "# typeCount: " + typeCount + "\n";
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

        String formatPrometheus() {
            return "# entityCount: " + entityCount + "\n" +
                    "# relationCount: " + relationCount + "\n" +
                    "# attributeCount: " + attributeCount + "\n" +
                    "# hasCount: " + hasCount + "\n" +
                    "# roleCount: " + roleCount + "\n" +
                    "# storageInBytes: " + storageInBytes + "\n" +
                    "# storageKeyCount: " + storageKeyCount + "\n";
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

        JsonObject asJSON(String database) {
            JsonObject load = new JsonObject();
            load.add("database", database);
            load.add("schema", schemaLoad.asJSON());
            load.add("data", dataLoad.asJSON());
            load.add("connection", connectionPeakCounts.asJSON());
            return load;
        }

        String formatPrometheus() {
            return schemaLoad.formatPrometheus() + "\n" + dataLoad.formatPrometheus();
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

        NetworkRequests() {
            for (var kind : Kind.values()) {
                successful.put(kind, new AtomicLong(0));
                failed.put(kind, new AtomicLong(0));
            }
        }

        public void takeCountsSnapshot() {
            successful.replaceAll((kind, value) -> new AtomicLong(0));
            failed.replaceAll((kind, value) -> new AtomicLong(0));
        }

        public void success(Kind kind) {
            successful.get(kind).incrementAndGet();
        }

        public void fail(Kind kind) {
            failed.get(kind).incrementAndGet();
        }

        JsonArray asJSON(@Nullable String database) {
            JsonArray requests = new JsonArray();

            for (var kind : Kind.values()) {
                long successfulValue = successful.get(kind).get();
                long failedValue = failed.get(kind).get();
                if (successfulValue == 0 && failedValue == 0) {
                    continue;
                }

                JsonObject requestObject = new JsonObject();
                requestObject.add("name", kind.name());
                if (database != null) {
                    requestObject.add("database", database);
                }
                requestObject.add("successful", successfulValue);
                requestObject.add("failed", failedValue);
                requests.add(requestObject);
            }

            return requests;
        }

        String formatPrometheus(String databaseName) {
            StringBuilder buf = new StringBuilder("# TYPE typedb_attempted_requests_total counter\n");
            for (var kind : Kind.values()) {
                var attempted = successful.get(kind).get() + failed.get(kind).get();
                buf.append("typedb_attempted_requests_total{databaseName=\"").append(databaseName).append("\", kind=\"").append(kind).append("\"} ").append(attempted).append("\n");
            }
            buf.append("\n# TYPE typedb_successful_requests_total counter\n");
            for (var kind : Kind.values()) {
                buf.append("typedb_successful_requests_total{databaseName=\"").append(databaseName).append("\", kind=\"").append(kind).append("\"} ").append(successful.get(kind)).append("\n");
            }
            return buf.toString();
        }
    }

    private static class UserErrorStatistics {
        private final ConcurrentMap<String, AtomicLong> errorCounts = new ConcurrentHashMap<>();
        private ConcurrentMap<String, AtomicLong> errorCountsSnapshot = new ConcurrentHashMap<>();

        public void takeCountsSnapshot() {
            errorCounts.clear();
        }

        public void register(String errorCode) {
            errorCounts.computeIfAbsent(errorCode, c -> new AtomicLong(0)).incrementAndGet();
        }

        JsonArray asJSON(@Nullable String database) {
            JsonArray errors = new JsonArray();

            for (String code : errorCounts.keySet()) {
                JsonObject errorObject = new JsonObject();
                errorObject.add("code", code);
                if (database != null) {
                    errorObject.add("database", database);
                }
                errorObject.add("count", errorCounts.get(code).get());
                errors.add(errorObject);
            }

            return errors;
        }

        String formatPrometheus(String databaseName) {
            if (errorCounts.isEmpty()) return "";

            StringBuilder buf = new StringBuilder("# TYPE typedb_error_total counter\n");
            for (String code : errorCounts.keySet()) {
                long count = errorCounts.get(code).get();
                buf.append("typedb_error_total{databaseName=\"").append(databaseName).append("\", code=\"").append(code).append("\"} ").append(count).append("\n");
            }
            return buf.toString();
        }
    }
}
