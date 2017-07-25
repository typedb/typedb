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

    private final String startRedisCmd;
    private final String stopRedisCmd;
    private final String isRedisRunningCmd;

    public RedisSupervisor(OperatingSystemCalls osCalls, String baseWorkDir) {
        this.osCalls = osCalls;

        if (osCalls.isMac()) { // use OSX binary, if the system is OSX
            this.startRedisCmd = baseWorkDir + "bin/redis-server-osx " + baseWorkDir + "conf/redis/redis.conf";
            this.stopRedisCmd = baseWorkDir + "bin/redis-cli-osx shutdown";

        } else { // otherwise assume it's Linux. TODO: Support Windows?
            this.startRedisCmd = baseWorkDir + "bin/redis-server-linux " + baseWorkDir + "conf/redis/redis.conf";
            this.stopRedisCmd = baseWorkDir + "bin/redis-cli-linux shutdown";
        }

        this.isRedisRunningCmd = "echo $(ps -ef | grep redis-server | grep -v grep | awk '{ print $2}')";

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
        int status = osCalls.execAndReturn(new String[] { "sh", "-c", startRedisCmd } );
        if (status != 0) throw new ExternalComponentException("unable to start redis - " + startRedisCmd);
    }

    public void stop() throws IOException, InterruptedException {
        int status = osCalls.execAndReturn(new String[] { "sh", "-c", stopRedisCmd} );
        if (status != 0) throw new ExternalComponentException("unable to stop redis - " + stopRedisCmd);
    }

    public boolean isRunning() throws IOException, InterruptedException {
        Process isRedisRunning = osCalls.exec(new String[] { "sh", "-c", isRedisRunningCmd } );

        int status = isRedisRunning.waitFor();
        if (status != 0) throw new ExternalComponentException("unable to run redis cli - " + isRedisRunningCmd);

        String output = osCalls.readStdoutFromProcess(isRedisRunning);
        boolean running = !output.equals("");
        return running;
    }
}