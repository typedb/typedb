/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import com.eclipsesource.json.JsonObject;

import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    private final SystemProperties system;
    private final NetworkRequests requests;
    private final CurrentCounts usage;
    private final UserErrorStatistics userErrors;

    public Metrics(String deploymentID, String serverID, String name, String version) {
        this.system = new SystemProperties(deploymentID, serverID, name, version);
        this.requests = new NetworkRequests();
        this.usage = new CurrentCounts();
        this.userErrors = new UserErrorStatistics();
    }

    public void requestAttempt(Metrics.NetworkRequests.Kind kind) {
        requests.attempt(kind);
    }

    public void requestSuccess(Metrics.NetworkRequests.Kind kind) {
        requests.success(kind);
    }

    public void setCurrentCount(CurrentCounts.Kind kind, long value) {
        usage.set(kind, value);
    }

    public void registerError(String errorCode) {
        userErrors.register(errorCode);
    }

    protected String formatPrometheus() {
        return String.join("\n", system.formatPrometheus(), requests.formatPrometheus(), usage.formatPrometheus(), userErrors.formatPrometheus());
    }

    protected JsonObject asJSON() {
        JsonObject metrics = new JsonObject();
        metrics.add("system", system.asJSON());
        metrics.add("requests", requests.asJSON());
        metrics.add("DB usage", usage.asJSON());
        metrics.add("user errors", userErrors.asJSON());
        return metrics;
    }

    protected String formatJSON() {
        return asJSON().toString();
    }

    static class SystemProperties {
        private final String deploymentID;
        private final String serverID;
        private final String name;
        private final String version;

        SystemProperties(String deploymentID, String serverID, String name, String version) {
            this.deploymentID = deploymentID;
            this.serverID = serverID;
            this.name = name;
            this.version = version;
        }

        JsonObject asJSON() {
            JsonObject system = new JsonObject();
            system.add("TypeDB version", name + " " + version);
            system.add("Deployment ID", deploymentID);
            system.add("Server ID", serverID);
            system.add("Time zone", TimeZone.getDefault().getID());
            system.add("Java version", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
            system.add("Platform", System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version"));
            return system;
        }

        String formatPrometheus() {
            return "# TypeDB version: " + name + " " + version + "\n" +
                    // no deploymentID and serverID, that's for reporting only
                    "# Time zone: " + TimeZone.getDefault() .getID() + "\n" +
                    "# Java version: " + System.getProperty("java.vendor") + " " + System.getProperty("java.version") + "\n" +
                    "# Platform: " + System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version") + "\n";
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

        private final ConcurrentMap<Kind, AtomicLong> attempted = new ConcurrentHashMap<>();
        private final ConcurrentMap<Kind, AtomicLong> successful = new ConcurrentHashMap<>();

        NetworkRequests() {
            for (var kind : Kind.values()) {
                attempted.put(kind, new AtomicLong(0));
                successful.put(kind, new AtomicLong(0));
            }
        }

        public void attempt(Kind kind) {
            attempted.get(kind).incrementAndGet();
        }

        public void success(Kind kind) {
            successful.get(kind).incrementAndGet();
        }

        JsonObject asJSON() {
            JsonObject requests = new JsonObject();
            for (var kind : Kind.values()) {
                JsonObject requestStats = new JsonObject();
                requestStats.add("attempted", attempted.get(kind).get());
                requestStats.add("successful", successful.get(kind).get());
                requests.add(kind.name(), requestStats);
            }
            return requests;
        }

        String formatPrometheus() {
            StringBuilder buf = new StringBuilder("# TYPE typedb_attempted_requests_total counter\n");
            for (var kind : Kind.values()) {
                buf.append("typedb_attempted_requests_total{kind=\"").append(kind).append("\"} ").append(attempted.get(kind)).append("\n");
            }
            buf.append("\n# TYPE typedb_successful_requests_total counter\n");
            for (var kind : Kind.values()) {
                buf.append("typedb_successful_requests_total{kind=\"").append(kind).append("\"} ").append(successful.get(kind)).append("\n");
            }
            return buf.toString();
        }
    }

    public static class CurrentCounts {
        public enum Kind {
            DATABASES, SESSIONS, TRANSACTIONS, USERS,
        }

        private final ConcurrentMap<Kind, AtomicLong> counts = new ConcurrentHashMap<>();

        CurrentCounts() {
            for (Kind kind : Kind.values()) {
                counts.put(kind, new AtomicLong(0));
            }
        }

        public void set(Kind kind, long value) {
            counts.get(kind).set(value);
        }

        JsonObject asJSON() {
            JsonObject current = new JsonObject();
            for (Kind kind : Kind.values()) {
                current.add(kind.name(), counts.get(kind).get());
            }
            return current;
        }

        String formatPrometheus() {
            StringBuilder buf  = new StringBuilder("# TYPE typedb_current_count gauge\n");
            for (Kind kind : Kind.values()) {
                buf.append("typedb_current_count{kind=\"").append(kind).append("\"} ").append(counts.get(kind)).append("\n");
            }
            return buf.toString();
        }
    }

    private static class UserErrorStatistics {
        private final ConcurrentMap<String, AtomicLong> errorCounts = new ConcurrentHashMap<>();

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

        String formatPrometheus() {
            if (errorCounts.isEmpty()) return "";

            StringBuilder buf = new StringBuilder("# TYPE typedb_error_total counter\n");
            for (String code : errorCounts.keySet()) {
                long count = errorCounts.get(code).get();
                buf.append("typedb_error_total{code=\"").append(code).append("\"} ").append(count).append("\n");
            }
            return buf.toString();
        }
    }
}
