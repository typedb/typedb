/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.externalcomponents;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Responsible for supervising cassandra.
 *
 * @author Ganeshwara Herawan Hananda
 */

public class RedisSupervisor {
    private final Logger LOG = Logger.getLogger(RedisSupervisor.class.getName());
    private OperatingSystemCalls osCalls;

    public RedisSupervisor(OperatingSystemCalls osCalls) {
        this.osCalls = osCalls;
    }

    public void startIfNotRunning() throws IOException, InterruptedException {
        LOG.info("checking if there exists a running redis process...");
        if (!isRunning()) {
            LOG.info("redis isn't yet running. attempting to start...");
            start();
        } else {
            LOG.info("found an existing redis process.");
        }
    }

    public void stopIfRunning() throws IOException, InterruptedException {
        LOG.info("checking if there exists a running redis process...");
        if (isRunning()) {
            LOG.info("a redis process found. attempting to stop...");
            stop();
            LOG.info("redis has been stopped.");
        } else {
            LOG.info("no redis process is found.");
        }
    }

    public void start() throws IOException, InterruptedException {
        if (osCalls.isMac()) {
            startRedisOsX();
        } else {
            startRedisLinux();
        }
    }

    public void stop() throws IOException, InterruptedException {
        if (osCalls.isMac()) {
            shutdownRedisOsX();
        }
        else {
            shutdownRedisLinux();
        }
    }

    // TODO: fix path
    public void startRedisOsX() throws IOException, InterruptedException {
        Process ps = osCalls.exec(new String[] { "sh", "-c", "bin/redis-server-osx conf/redis/redis.conf"} );
        System.out.println("=== " + ps.waitFor() + " ===");
    }

    // TODO: fix path
    public void startRedisLinux() throws IOException, InterruptedException {
        Process ps = osCalls.exec(new String[] { "sh", "-c", "bin/redis-server-linux conf/redis/redis.conf"} );
        System.out.println("=== " + ps.waitFor() + " ===");
    }

    public void shutdownRedisOsX() throws IOException, InterruptedException {
        Process ps = osCalls.exec(new String[] { "sh", "-c", "bin/redis-cli-osx shutdown"} );
        ps.waitFor();
    }

    public void shutdownRedisLinux() throws IOException, InterruptedException {
        Process ps = osCalls.exec(new String[] { "sh", "-c", "bin/redis-cli-linux shutdown"} );
        ps.waitFor();
    }

    public boolean isRunning() throws IOException, InterruptedException {
        Process ps = osCalls.exec(new String[] { "sh", "-c", "echo $(ps -ef | grep redis-server | grep -v grep | awk '{ print $2}')"} );
        String output = osCalls.readStdoutFromProcess(ps);
        boolean running = !output.equals("");
        return running;
    }
}
