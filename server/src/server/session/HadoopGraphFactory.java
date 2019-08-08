/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class produces a graph on top of {@link HadoopGraph}.
 * With this vendor some exceptions are in places:
 * 1. The Grakn API cannot work on {@link HadoopGraph} this is due to not being able to directly write to a
 * {@link HadoopGraph}.
 * 2. This factory primarily exists as a means of producing a
 * {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer} on of {@link HadoopGraph}
 */
public class HadoopGraphFactory {
    // Keep visibility to protected as this is used by KGMS
    protected Config config;
    //These properties are loaded in by default and can optionally be overwritten
    private static final Properties DEFAULT_OLAP_PROPERTIES;
    private static final String DEFAULT_OLAP_PATH = "resources/default-OLAP-configs.properties";
    private static final String STORAGE_HOSTNAME = ConfigKey.STORAGE_HOSTNAME.name();
    // Keep visibility to protected as this is used by KGMS
    protected static final String STORAGE_KEYSPACE = ConfigKey.STORAGE_KEYSPACE.name();
    // Keep visibility to protected as this is used by KGMS
    protected static final String JANUSGRAPHMR_IOFORMAT_CONF = "janusgraphmr.ioformat.conf.";

    static {
        DEFAULT_OLAP_PROPERTIES = new Properties();
        try (InputStream in = HadoopGraphFactory.class.getClassLoader().getResourceAsStream(DEFAULT_OLAP_PATH)) {
            DEFAULT_OLAP_PROPERTIES.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(DEFAULT_OLAP_PATH), e);
        }
    }

    public HadoopGraphFactory(Config config) {
        this.config = config;
        String hostnameValue = this.config.getProperty(ConfigKey.STORAGE_HOSTNAME);
        this.config.properties().setProperty(JANUSGRAPHMR_IOFORMAT_CONF + STORAGE_HOSTNAME, hostnameValue);
        //Load Defaults
        DEFAULT_OLAP_PROPERTIES.forEach((key, value) -> {
            if (!this.config.properties().containsKey(key)) {
                this.config.properties().put(key, value);
            }
        });
    }

    synchronized HadoopGraph getGraph(KeyspaceImpl keyspace) {
        return (HadoopGraph) GraphFactory.open(addHadoopProperties(keyspace.name()).properties());
    }

    /**
     * Clone Grakn config and adds OLAP specific keyspace property
     * @param keyspaceName keyspace value to add as a property
     * @return new copy of configuration, specific for current keyspace
     */
    private Config addHadoopProperties(String keyspaceName) {
        Config localConfig = Config.of(config.properties());
        localConfig.properties().setProperty(JANUSGRAPHMR_IOFORMAT_CONF + STORAGE_KEYSPACE, keyspaceName);
        return localConfig;
    }
}
