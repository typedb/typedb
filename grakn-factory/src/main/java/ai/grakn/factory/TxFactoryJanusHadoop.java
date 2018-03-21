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

package ai.grakn.factory;

import ai.grakn.GraknConfigKey;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *     A {@link ai.grakn.GraknTx} on top of {@link HadoopGraph}
 * </p>
 *
 * <p>
 *     This produces a graph on top of {@link HadoopGraph}.
 *     The base construction process defined by {@link TxFactoryAbstract} ensures the graph factories are singletons.
 *     With this vendor some exceptions are in places:
 *     1. The Grakn API cannnot work on {@link HadoopGraph} this is due to not being able to directly write to a
 *     {@link HadoopGraph}.
 *     2. This factory primarily exists as a means of producing a
 *     {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer} on of {@link HadoopGraph}
 * </p>
 *
 * @author fppt
 */
public class TxFactoryJanusHadoop extends TxFactoryAbstract<EmbeddedGraknTx<HadoopGraph>, HadoopGraph> {
    public static final String JANUSMR_IOFORMAT_CONF = "janusmr.ioformat.conf.";
    public static final String JANUSGRAPHMR_IOFORMAT_CONF = "janusgraphmr.ioformat.conf.";
    public static final String STORAGE_CASSANDRA_KEYSPACE = "storage.cassandra.keyspace";
    public static final String STORAGE_HOSTNAME = "storage.hostname";

    private static final String JANUSMR_IOFORMAT_CONF_STORAGE_CASSANDRA_KEYSPACE = JANUSMR_IOFORMAT_CONF + STORAGE_CASSANDRA_KEYSPACE;
    private static final String JANUSMR_IOFORMAT_CONF_STORAGE_HOSTNAME = JANUSMR_IOFORMAT_CONF + STORAGE_HOSTNAME;

    private static final String JANUSGRAPHMR_IOFORMAT_CONF_STORAGE_HOSTNAME = JANUSGRAPHMR_IOFORMAT_CONF + STORAGE_HOSTNAME;
    private static final String JANUSGRAPHMR_IOFORMAT_CONF_STORAGE_CASSANDRA_KEYSPACE = JANUSGRAPHMR_IOFORMAT_CONF + STORAGE_CASSANDRA_KEYSPACE;

    private static final String CASSANDRA_INPUT_KEYSPACE = "cassandra.input.keyspace";

    private final Logger LOG = LoggerFactory.getLogger(TxFactoryJanusHadoop.class);

    TxFactoryJanusHadoop(EmbeddedGraknSession session) {
        super(session);

        session().config().properties().setProperty(JANUSMR_IOFORMAT_CONF_STORAGE_CASSANDRA_KEYSPACE, session().keyspace().getValue());
        session().config().properties().setProperty(CASSANDRA_INPUT_KEYSPACE, session().keyspace().getValue());
        session().config().properties().setProperty(JANUSGRAPHMR_IOFORMAT_CONF_STORAGE_HOSTNAME, session().config().getProperty(GraknConfigKey.STORAGE_HOSTNAME));
        session().config().properties().setProperty(JANUSMR_IOFORMAT_CONF_STORAGE_HOSTNAME, session().config().getProperty(GraknConfigKey.STORAGE_HOSTNAME));
        session().config().properties().setProperty(JANUSGRAPHMR_IOFORMAT_CONF_STORAGE_CASSANDRA_KEYSPACE, session().keyspace().getValue());
    }

    @Override
    EmbeddedGraknTx<HadoopGraph> buildGraknGraphFromTinker(HadoopGraph graph) {
        throw new UnsupportedOperationException(ErrorMessage.CANNOT_PRODUCE_TX.getMessage(HadoopGraph.class.getName()));
    }

    @Override
    HadoopGraph buildTinkerPopGraph(boolean batchLoading) {
        LOG.warn("Hadoop graph ignores parameter address [" + session().uri() + "]");

        //Load Defaults
        TxFactoryJanus.DEFAULT_PROPERTIES.forEach((key, value) -> {
            if(!session().config().properties().containsKey(key)){
                session().config().properties().put(key, value);
            }
        });

        return (HadoopGraph) GraphFactory.open(session().config().properties());
    }

    //TODO: Get rid of the need for batch loading parameter
    @Override
    protected HadoopGraph getGraphWithNewTransaction(HadoopGraph graph, boolean batchloading) {
        return graph;
    }
}
