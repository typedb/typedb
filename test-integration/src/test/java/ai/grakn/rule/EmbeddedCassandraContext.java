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

import com.google.testing.junit.runner.util.GoogleTestSecurityManager;
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
 * Helper class for starting and working with an embedded cassandra.
 * This should be used for testing purposes only
 * </p>
 *
 */
public class EmbeddedCassandraContext extends ExternalResource {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmbeddedCassandraContext.class);
    private final static String DEFAULT_YAML_FILE_PATH = "cassandra-embedded.yaml";


    private static AtomicBoolean CASSANDRA_RUNNING = new AtomicBoolean(false);

    private static AtomicInteger IN_CASSANDRA_CONTEXT = new AtomicInteger(0);

    private String yamlFilePath;

    private EmbeddedCassandraContext(String yamlFilePath) {
        this.yamlFilePath = yamlFilePath;
        // Disable Google SecurityManager because Cassandra uses System.exit()
        GoogleTestSecurityManager.uninstallIfInstalled();
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
    protected void before() {
//        System.setSecurityManager(new SecurityManager() {
//            @Override
//            public void checkPermission(Permission perm) {
//            }
//
//            @Override
//            public void checkPermission(Permission perm, Object context) {
//            }
//
//            @Override
//            public void checkExit(int status) {
//                String message = "System exit requested with error " + status;
//                throw new SecurityException(message);
//            }
//        });
        if (CASSANDRA_RUNNING.compareAndSet(false, true)) {
            try {
                LOG.info("starting cassandra...");
                EmbeddedCassandraServerHelper.startEmbeddedCassandra(yamlFilePath, 30_000L);
                LOG.info("cassandra started.");

            } catch (TTransportException | IOException | ConfigurationException e) {
                throw new RuntimeException("Cannot start Embedded Cassandra", e);
            }
        }
        IN_CASSANDRA_CONTEXT.incrementAndGet();
    }

    @Override
    protected void after() {
        IN_CASSANDRA_CONTEXT.decrementAndGet();
    }
}