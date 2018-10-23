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

package ai.grakn.test.rule;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * <p>
 * Starts Embedded Cassandra
 * </p>
 * <p>
 * Helper class for starting and working with an embedded cassandra.
 * This should be used for testing purposes only
 * </p>
 */
public class EmbeddedCassandraContext extends ExternalResource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmbeddedCassandraContext.class);
    private final static String DEFAULT_YAML_FILE_PATH = "test-integration/resources/cassandra-embedded.yaml";

    private String yamlFilePath;

    public EmbeddedCassandraContext() {
        this.yamlFilePath = DEFAULT_YAML_FILE_PATH;
        System.setProperty("java.security.manager", "nottodaypotato");
    }

    @Override
    protected void before() {
        try {
            LOG.info("starting cassandra...");
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(yamlFilePath, 30_000L);
            LOG.info("cassandra started.");
        } catch (TTransportException | IOException | ConfigurationException e) {
            throw new RuntimeException("Cannot start Embedded Cassandra", e);
        }
    }

    @Override
    protected void after() {
    }
}