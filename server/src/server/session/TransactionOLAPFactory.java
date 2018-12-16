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

import grakn.core.common.config.ConfigKey;
import grakn.core.server.Transaction;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Transaction} on top of {@link HadoopGraph}
 * This produces a graph on top of {@link HadoopGraph}.
 * With this vendor some exceptions are in places:
 * 1. The Grakn API cannnot work on {@link HadoopGraph} this is due to not being able to directly write to a
 * {@link HadoopGraph}.
 * 2. This factory primarily exists as a means of producing a
 * {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer} on of {@link HadoopGraph}
 */
public class TransactionOLAPFactory {

    private final Logger LOG = LoggerFactory.getLogger(TransactionOLAPFactory.class);
    private final SessionImpl session;
    private HadoopGraph graph = null;

    public TransactionOLAPFactory(SessionImpl sessionImpl) {
        this.session = sessionImpl;

        // Janus configurations
        String mrPrefixConf = "janusmr.ioformat.conf.";
        String graphMrPrefixConf = "janusgraphmr.ioformat.conf.";
        String inputKeyspaceConf = "cassandra.input.keyspace";
        String keyspaceConf = "storage.cassandra.keyspace";
        String hostnameConf = "storage.hostname";

        // Values
        String keyspaceValue = session.keyspace().getName();
        String hostnameValue = session.config().getProperty(ConfigKey.STORAGE_HOSTNAME);

        session.config().properties().setProperty(mrPrefixConf + keyspaceConf, keyspaceValue);
        session.config().properties().setProperty(mrPrefixConf + hostnameConf, hostnameValue);
        session.config().properties().setProperty(graphMrPrefixConf + hostnameConf, hostnameValue);
        session.config().properties().setProperty(graphMrPrefixConf + keyspaceConf, keyspaceValue);
        session.config().properties().setProperty(inputKeyspaceConf, keyspaceValue);
    }

    public TransactionOLAP openOLAP() {
        return new TransactionOLAP(getGraph());
    }

    public synchronized HadoopGraph getGraph() {
        if (graph == null) {
            LOG.warn("Hadoop graph ignores parameter address.");

            //Load Defaults
            TransactionOLTPFactory.getDefaultProperties().forEach((key, value) -> {
                if (!session.config().properties().containsKey(key)) {
                    session.config().properties().put(key, value);
                }
            });

            graph = (HadoopGraph) GraphFactory.open(session.config().properties());
        }

        return graph;
    }
}
