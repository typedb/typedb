/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import com.eclipsesource.json.JsonObject;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    private final BaseProperties base;
    private final ServerStaticProperties serverStatic;
    private final ServerDynamicProperties serverDynamic;
    private ConcurrentMap<String, NetworkRequests> requests = new ConcurrentHashMap<>();
    private ConcurrentMap<String, CurrentCounts> usage = new ConcurrentHashMap<>();
    private ConcurrentMap<String, UserErrorStatistics> userErrors = new ConcurrentHashMap<>();

    public Metrics(String deploymentID, String serverID, String name, String version, boolean reportingEnabled, Path dbDirectory) {
        this.base = new BaseProperties(deploymentID, serverID, name, reportingEnabled);
        this.serverStatic = new ServerStaticProperties();
        this.serverDynamic = new ServerDynamicProperties(version, dbDirectory);
    }

    public void takeCountsSnapshot() {
        for (var databaseName : this.requests.keySet()) {
            this.requests.get(databaseName).takeCountsSnapshot();
            this.usage.get(databaseName).takeCountsSnapshot();
            this.userErrors.get(databaseName).takeCountsSnapshot();
        }
    }

    public void addDatabaseIfAbsent(String databaseName) {
        if (!this.requests.containsKey(databaseName)) {
            this.requests.put(databaseName, new NetworkRequests());
        }

        if (!this.usage.containsKey(databaseName)) {
            this.usage.put(databaseName, new CurrentCounts());
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

    public void setCurrentCount(String databaseName, CurrentCounts.Kind kind, long value) {
        addDatabaseIfAbsent(databaseName);
        usage.get(databaseName).set(kind, value);
    }

    public void registerError(String databaseName, String errorCode) {
        addDatabaseIfAbsent(databaseName);
        userErrors.get(databaseName).register(errorCode);
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
                    usage.get(databaseName).formatPrometheus(databaseName),
                    userErrors.get(databaseName).formatPrometheus(databaseName));
        }
        return
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
        for (var databaseName : this.requests.keySet()) {
            metrics.add("requests", requests.get(databaseName).asJSON());
            metrics.add("DB usage", usage.get(databaseName).asJSON());
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

        ServerDynamicProperties(String version, Path dbDirectory) {
            this.version = version;
            this.dbRoot = dbDirectory.toFile();
            System.out.println("HERE IS MY DB ROOT: " + this.dbRoot + " or DIR: " + dbDirectory); // TODO: Remove after testing!
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
            USER_MANAGEMENT,
            USER,
            DATABASE_MANAGEMENT,
            DATABASE,
            SESSION,
            TRANSACTION,
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
                var attempted = successful.get(kind).getAndAdd(failed.get(kind).get());
                buf.append("typedb_attempted_requests_total{databaseName=\"").append(databaseName).append("\", kind=\"").append(kind).append("\"} ").append(attempted).append("\n");
            }
            buf.append("\n# TYPE typedb_successful_requests_total counter\n");
            for (var kind : Kind.values()) {
                buf.append("typedb_successful_requests_total{databaseName=\"").append(databaseName).append("\", kind=\"").append(kind).append("\"} ").append(successful.get(kind)).append("\n");
            }
            return buf.toString();
        }
    }

    public static class CurrentCounts {
        public enum Kind {
            DATABASES, SESSIONS, TRANSACTIONS, USERS,
        }

        private final ConcurrentMap<Kind, AtomicLong> counts = new ConcurrentHashMap<>();
        private final ConcurrentMap<Kind, AtomicLong> maxCounts = new ConcurrentHashMap<>();

        CurrentCounts() {
            for (Kind kind : Kind.values()) {
                counts.put(kind, new AtomicLong(0)); // TODO: calculate diff instead of dropping?
                maxCounts.put(kind, new AtomicLong(0));
            }
        }

        public void takeCountsSnapshot() {
            maxCounts.replaceAll((kind, value) -> new AtomicLong(counts.get(kind).get()));
        }

        public void set(Kind kind, long value) {
            counts.get(kind).set(value);

            if (maxCounts.get(kind).get() < value) {
                maxCounts.get(kind).set(value);
            }
        }

        JsonObject asJSON() {
            JsonObject current = new JsonObject();
            for (Kind kind : Kind.values()) {
                current.add(kind.name(), maxCounts.get(kind).get());
            }
            return current;
        }

        String formatPrometheus(String databaseName) {
            StringBuilder buf = new StringBuilder("# TYPE typedb_peak_count gauge\n");
            for (Kind kind : Kind.values()) {
                buf.append("typedb_peak_count{databaseName=\"").append(databaseName).append("\", kind=\"").append(kind).append("\"} ").append(maxCounts.get(kind)).append("\n");
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
