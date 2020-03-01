/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;

/**
 * CassandraDaemon Wrapper that persists Cassandra PID to file once the service is up and running
 */
public class GraknStorage {
    private static final Logger LOG = LoggerFactory.getLogger(GraknStorage.class);

    public static void main(String[] args) {
        try {
            CassandraDaemon instance = new CassandraDaemon();
            instance.activate();
            persistPID();
        } catch (Exception e) {
            LOG.error("Cassandra Exception:", e);
            System.err.println(e.getMessage());
        }
    }

    private static void persistPID() {
        String pidString = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        try {
            String pidFile = System.getProperty("cassandra-pidfile");
            if (pidFile == null) {
                LOG.warn("Directory for Cassandra PID not provided, the PID will not be persisted.");
                return;
            }
            PrintWriter writer = new PrintWriter(pidFile, "UTF-8");
            writer.print(pidString);
            writer.close();
        } catch (IOException e) {
            LOG.error("Error persisting storage PID:{}", e.getMessage());
        }
    }
}
