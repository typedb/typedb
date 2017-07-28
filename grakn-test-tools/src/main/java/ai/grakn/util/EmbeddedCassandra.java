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

package ai.grakn.util;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *     Starts Embedded Cassandra
 * </p>
 *
 * <p>
 *     Helper class for starting and working with an embedded cassandra.
 *     This should be used for testing purposes only
 * </p>
 *
 * @author fppt
 *
 */
public class EmbeddedCassandra {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmbeddedCassandra.class);
    private static AtomicBoolean CASSANDRA_RUNNING = new AtomicBoolean(false);

    /**
     * Starts an embedded version of cassandra
     */
    public static void start(){
        if(CASSANDRA_RUNNING.compareAndSet(false, true)) {
            try {
                LOG.info("starting cassandra...");
                EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml", 30_000L);
                EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
                //This thread sleep is to give time for cass to startup
                //TODO: Determine if this is still needed
                try {
                    Thread.sleep(5000);
                }
                catch(InterruptedException ex) {
                    LOG.info("Thread sleep interrupted.");
                }
                LOG.info("cassandra started.");

            } catch (TTransportException | IOException | ConfigurationException e) {
                throw new RuntimeException("Cannot start Embedded Cassandra", e);
            }
        }
    }
}
