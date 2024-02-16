/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
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
    private final UsageStatistics usage;

    Metrics(String serverID, String name, String version) {
        this.system = new SystemProperties(serverID, name, version);
        this.requests = new NetworkRequests();
        this.usage = new UsageStatistics();
    }

    public void requestAttempt(Metrics.NetworkRequests.Kind kind) {
        requests.attempt(kind);
    }

    public void requestSuccess(Metrics.NetworkRequests.Kind kind) {
        requests.success(kind);
    }

    public void setGauge(Metrics.UsageStatistics.Kind kind, long value) {
        usage.set(kind, value);
    }

    String formatPrometheus() {
        return system.formatPrometheus() + "\n" + requests.formatPrometheus() + "\n" + usage.formatPrometheus();
    }

    String formatJSON() {
        JsonObject metrics = new JsonObject();
        metrics.add("system", system.formatJSON());
        metrics.add("requests", requests.formatJSON());
        metrics.add("current", usage.formatJSON());
        return metrics.toString();
    }

    static class SystemProperties {
        private final String serverID;
        private final String name;
        private final String version;

        SystemProperties(String serverID, String name, String version) {
            this.serverID = serverID;
            this.name = name;
            this.version = version;
        }

        JsonObject formatJSON() {
            JsonObject system = new JsonObject();
            system.add("TypeDB version", name + " " + version);
            system.add("Server ID", serverID);
            system.add("Time zone", TimeZone.getDefault().getID());
            system.add("Java version", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
            system.add("Platform", System.getProperty("os.name") + " " + System.getProperty("os.arch") + " " + System.getProperty("os.version"));
            return system;
        }

        String formatPrometheus() {
            return "# TypeDB version: " + name + " " + version + "\n" +
                    // no serverID, that's for reporting only
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

        JsonObject formatJSON() {
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
            StringBuilder buf = new StringBuilder("# TYPE attempted_requests_total counter\n");
            for (var kind : Kind.values()) {
                buf.append("attempted_requests_total{kind=\"").append(kind).append("\"} ").append(attempted.get(kind)).append("\n");
            }
            buf.append("\n# TYPE successful_requests_total counter\n");
            for (var kind : Kind.values()) {
                buf.append("successful_requests_total{kind=\"").append(kind).append("\"} ").append(successful.get(kind)).append("\n");
            }
            return buf.toString();
        }
    }

    public static class UsageStatistics {
        public enum Kind {
            DATABASE_COUNT,
            SESSION_COUNT,
            TRANSACTION_COUNT,
        }

        private final ConcurrentMap<Kind, AtomicLong> gauges = new ConcurrentHashMap<>();

        UsageStatistics() {
            for (Kind kind : Kind.values()) {
                gauges.put(kind, new AtomicLong(0));
            }
        }

        public void set(Kind kind, long value) {
            gauges.get(kind).addAndGet(value);
        }

        JsonObject formatJSON() {
            JsonObject current = new JsonObject();
            for (Kind kind : Kind.values()) {
                current.add(kind.name(), gauges.get(kind).get());
            }
            return current;
        }

        String formatPrometheus() {
            StringBuilder buf  = new StringBuilder("# TYPE current_count gauge\n");
            for (Kind kind : Kind.values()) {
                buf.append("current_count{kind=\"").append(kind).append("\"} ").append(gauges.get(kind)).append("\n");
            }
            return buf.toString();
        }
    }
}
