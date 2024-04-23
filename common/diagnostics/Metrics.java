/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import com.eclipsesource.json.JsonObject;
import com.vaticle.typedb.core.common.parameters.Arguments;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    private final BaseProperties base;
    private final ServerStaticProperties serverStatic;
    private final ServerDynamicProperties serverDynamic;
    private ConcurrentMap<String, DatabaseSchemaLoad> databaseSchemaLoad = new ConcurrentHashMap<>();
    private ConcurrentMap<String, DatabaseDataLoad> databaseDataLoad = new ConcurrentHashMap<>();
    private ConcurrentMap<String, ConnectionPeakCounts> connectionPeakCounts = new ConcurrentHashMap<>();
    private ConcurrentMap<String, NetworkRequests> requests = new ConcurrentHashMap<>();
    private ConcurrentMap<String, UserErrorStatistics> userErrors = new ConcurrentHashMap<>();

    public Metrics(String deploymentID, String serverID, String name, String version, boolean reportingEnabled, Path dataDirectory) {
        this.base = new BaseProperties(deploymentID, serverID, name, reportingEnabled);
        this.serverStatic = new ServerStaticProperties();
        this.serverDynamic = new ServerDynamicProperties(version, dataDirectory);
    }

    public void takeCountsSnapshot() {
        for (var databaseName : this.requests.keySet()) {
            this.requests.get(databaseName).takeCountsSnapshot();
            this.connectionPeakCounts.get(databaseName).takeCountsSnapshot();
            this.userErrors.get(databaseName).takeCountsSnapshot();
        }
    }

    public void addDatabaseIfAbsent(String databaseName) {
        if (!this.requests.containsKey(databaseName)) {
            this.requests.put(databaseName, new NetworkRequests());
        }

        if (!this.connectionPeakCounts.containsKey(databaseName)) {
            this.connectionPeakCounts.put(databaseName, new ConnectionPeakCounts());
        }

        if (!this.userErrors.containsKey(databaseName)) {
            this.userErrors.put(databaseName, new UserErrorStatistics());
        }
    }

    public void requestSuccess(String databaseName, Metrics.NetworkRequests.Kind kind) {
        addDatabaseIfAbsent(databaseName);
        requests.get(databaseName).success(kind);
    }

    public void requestFail(String databaseName, Metrics.NetworkRequests.Kind kind) {
        addDatabaseIfAbsent(databaseName);
        requests.get(databaseName).fail(kind);
    }

    public void incrementCurrentCount(String databaseName, ConnectionPeakCounts.Kind kind) {
        addDatabaseIfAbsent(databaseName);
        connectionPeakCounts.get(databaseName).incrementCurrent(kind);
    }

    public void decrementCurrentCount(String databaseName, ConnectionPeakCounts.Kind kind) {
        addDatabaseIfAbsent(databaseName);
        connectionPeakCounts.get(databaseName).decrementCurrent(kind);
    }

    public void setCurrentCount(String databaseName, ConnectionPeakCounts.Kind kind, long value) {
        addDatabaseIfAbsent(databaseName);
        connectionPeakCounts.get(databaseName).setCurrent(kind, value);
    }

    public void registerError(String databaseName, String errorCode) {
        addDatabaseIfAbsent(databaseName);
        userErrors.get(databaseName).register(errorCode);
    }

    public void submitDatabaseDiagnostics(String databaseName, Metrics.DatabaseSchemaLoad schemaLoad, Metrics.DatabaseDataLoad dataLoad) {
        addDatabaseIfAbsent(databaseName);
        databaseSchemaLoad.put(databaseName, schemaLoad);
        databaseDataLoad.put(databaseName, dataLoad);
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

    protected JsonObject asJSON(boolean reporting) {
        JsonObject metrics = base.asJSON();

        if (reporting && !base.getReportingEnabled()) {
            return metrics;
        }

        for (var record : serverStatic.asJSON()) {
            metrics.add(record.getName(), record.getValue());
        }

        metrics.add("server", serverDynamic.asJSON());

        metrics.add("usage", "..."); // TODO: Create json object wrapping this data

        for (String databaseName : databaseSchemaLoad.keySet()) {
            JsonObject load = new JsonObject();
            load.add("schema", databaseSchemaLoad.get(databaseName).asJSON());
            load.add("data", databaseDataLoad.get(databaseName).asJSON());
            load.add("connection", connectionPeakCounts.get(databaseName).asJSON());
            metrics.add("load", load);

            metrics.add("requests", requests.get(databaseName).asJSON());
            metrics.add("user errors", userErrors.get(databaseName).asJSON());
        }

        return metrics;
    }

    protected String formatJSON(boolean reporting) {
        return asJSON(reporting).toString();
    }

    static class BaseProperties {
        private final String deploymentID;
        private final String serverID;
        private final String distribution;
        private final boolean reportingEnabled;

        BaseProperties(String deploymentID, String serverID, String distribution, boolean reportingEnabled) {
            this.deploymentID = deploymentID;
            this.serverID = serverID;
            this.distribution = distribution;
            this.reportingEnabled = reportingEnabled;
        }

        JsonObject asJSON() {
            JsonObject system = new JsonObject();
            system.add("deploymentID", deploymentID);
            system.add("serverID", serverID);
            system.add("distribution", distribution);
            system.add("timestamp", LocalDateTime.now(ZoneOffset.UTC).toString());
            system.add("periodInSeconds", 1 * 3600); // TODO: DELAY_IN_HOURS = ...
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

        JsonObject asJSON() {
            JsonObject requests = new JsonObject();
            for (var kind : Kind.values()) {
                JsonObject requestStats = new JsonObject();
                requestStats.add("successful", successful.get(kind).get());
                requestStats.add("failed", failed.get(kind).get());
                requests.add(kind.name(), requestStats);
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

    public static class DatabaseSchemaLoad {
        public long typeCount;

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
        public long entityCount;
        public long relationCount;
        public long attributeCount;
        public long hasCount;
        public long roleCount;
        public long storageInBytes;
        public long storageKeyCount;

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
            CONNECTIONS("connectionPeakCount"), // TODO: Remove as it can't be collected
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
                peak.add(kind.getJsonName(), peakCounts.get(kind).get());
            }
            return peak;
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

        JsonObject asJSON() {
            if (errorCounts.isEmpty()) return new JsonObject();

            JsonObject errors = new JsonObject();
            for (String code : errorCounts.keySet()) {
                long count = errorCounts.get(code).get();
                errors.add(code, count);
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
