/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.test.rule;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * Starts Embedded Cassandra
 * </p>
 * <p>
 * <p>
 * Helper class for starting and working with an embedded cassandra.
 * This should be used for testing purposes only
 * </p>
 *
 * @author fppt
 */
public class EmbeddedCassandraContext extends ExternalResource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmbeddedCassandraContext.class);
    private static final String DEFAULT_YAML_FILE_PATH = "cassandra-embedded.yaml";

    private static AtomicBoolean CASSANDRA_RUNNING = new AtomicBoolean(false);

    private static AtomicInteger IN_CASSANDRA_CONTEXT = new AtomicInteger(0);

    private String yamlFilePath;

    private EmbeddedCassandraContext(String yamlFilePath) {
        this.yamlFilePath = yamlFilePath;
    }

    public static EmbeddedCassandraContext create() {
        return new EmbeddedCassandraContext(DEFAULT_YAML_FILE_PATH);
    }

    public static EmbeddedCassandraContext create(String yamlFilePath) {
        return new EmbeddedCassandraContext(yamlFilePath);
    }


    public static boolean inCassandraContext() {
        return IN_CASSANDRA_CONTEXT.get() > 0;
    }

    @Override
    protected void before() throws Throwable {
        if (CASSANDRA_RUNNING.compareAndSet(false, true)) {
            try {
                LOG.info("starting cassandra...");
                EmbeddedCassandraServerHelper.startEmbeddedCassandra(yamlFilePath, 30_000L);
                //This thread sleep is to give time for cass to startup
                //TODO: Determine if this is still needed
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    LOG.info("Thread sleep interrupted.");
                    Thread.currentThread().interrupt();
                }
                LOG.info("cassandra started.");

            } catch (TTransportException | IOException e) {
                throw new RuntimeException("Cannot start Embedded Cassandra", e);
            } catch (ConfigurationException e) {
                LOG.error("Cassandra already running! Attempting to continue.");
            }
        }
        IN_CASSANDRA_CONTEXT.incrementAndGet();
    }

    @Override
    protected void after() {
        IN_CASSANDRA_CONTEXT.decrementAndGet();
    }
}
