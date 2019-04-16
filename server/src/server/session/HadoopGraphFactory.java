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

package grakn.core.server.session;

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.server.keyspace.KeyspaceImpl;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class produces a graph on top of {@link HadoopGraph}.
 * With this vendor some exceptions are in places:
 * 1. The Grakn API cannnot work on {@link HadoopGraph} this is due to not being able to directly write to a
 * {@link HadoopGraph}.
 * 2. This factory primarily exists as a means of producing a
 * {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer} on of {@link HadoopGraph}
 */
public class HadoopGraphFactory {

    private final Logger LOG = LoggerFactory.getLogger(HadoopGraphFactory.class);
    private Config config;
    private HadoopGraph graph = null;
    //These properties are loaded in by default and can optionally be overwritten
    private static final Properties DEFAULT_PROPERTIES;

    static {
        String DEFAULT_CONFIG = "resources/default-configs.properties";
        DEFAULT_PROPERTIES = new Properties();
        try (InputStream in = HadoopGraphFactory.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG)) {
            DEFAULT_PROPERTIES.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(DEFAULT_CONFIG), e);
        }
    }

    public HadoopGraphFactory(Config config, KeyspaceImpl keyspace) {
        this.config = addHadoopProperties(config, keyspace);
    }

    public synchronized HadoopGraph getGraph() {
        if (graph == null) {
            LOG.warn("Hadoop graph ignores parameter address.");

            //Load Defaults
            DEFAULT_PROPERTIES.forEach((key, value) -> {
                if (!config.properties().containsKey(key)) {
                   config.properties().put(key, value);
                }
            });

            graph = (HadoopGraph) GraphFactory.open(config.properties());
        }

        return graph;
    }

    protected Config addHadoopProperties(Config config, KeyspaceImpl keyspace) {
        // Janus configurations
        String graphMrPrefixConf = "janusgraphmr.ioformat.conf.";
        String inputKeyspaceConf = "cassandra.input.keyspace";
        String keyspaceConf = "storage.cassandra.keyspace";
        String hostnameConf = "storage.hostname";

        // Values
        String keyspaceValue = keyspace.name();
        String hostnameValue = config.getProperty(ConfigKey.STORAGE_HOSTNAME);

        config.properties().setProperty(graphMrPrefixConf + hostnameConf, hostnameValue);
        config.properties().setProperty(graphMrPrefixConf + keyspaceConf, keyspaceValue);
        config.properties().setProperty(inputKeyspaceConf, keyspaceValue);

        return config;
    }
}
