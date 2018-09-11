/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.engine.bootup;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;

/**
 * CassandraDaemon Wrapper that persists Cassandra PID to file once the service is up and running
 */

public class GraknCassandra {

    public static void main(String[] args) {
        try {
            CassandraDaemon instance = new CassandraDaemon();
            instance.activate();
            persistPID();
        }catch (Exception e){
            System.err.println("Exception while starting Cassandra:");
            System.err.println(e.getMessage());
            System.err.println("Check log file for more details.");
        }
    }

    private static void persistPID() {
        String pidString = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        try {
            String pidFile = System.getProperty("cassandra-pidfile");
            PrintWriter writer = new PrintWriter(pidFile, "UTF-8");
            writer.print(pidString);
            writer.close();
        } catch (IOException e) {
            System.err.println("Error persisting storage PID:" + e.getMessage());
        }
    }
}
