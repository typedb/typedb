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

package ai.grakn.factory;

import ai.grakn.GraknConfigKey;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.Map;

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

    /**
     * This map is used to override hidden config files.
     * The key of the map refers to the Janus configuration to be overridden
     * The value of the map specifies the value that will be injected
     */
    private static Map<String, String> overrideMap(EmbeddedGraknSession session) {
        // Janus configurations
        String mrPrefixConf = "janusmr.ioformat.conf.";
        String graphMrPrefixConf = "janusgraphmr.ioformat.conf.";
        String inputKeyspaceConf = "cassandra.input.keyspace";
        String keyspaceConf = "storage.cassandra.keyspace";
        String hostnameConf = "storage.hostname";

        // Values
        String keyspaceValue = session.keyspace().getValue();
        String hostnameValue = session.config().getProperty(GraknConfigKey.STORAGE_HOSTNAME);

        // build override map
        return ImmutableMap.of(
                mrPrefixConf + keyspaceConf, keyspaceValue,
                mrPrefixConf + hostnameConf, hostnameValue,
                graphMrPrefixConf + hostnameConf, hostnameValue,
                graphMrPrefixConf + keyspaceConf, keyspaceValue,
                inputKeyspaceConf, keyspaceValue
        );
    }

    TxFactoryJanusHadoop(EmbeddedGraknSession session) {
        super(session);

        overrideMap(session()).forEach((k, v) -> session().config().properties().setProperty(k, v));
    }

    @Override
    protected EmbeddedGraknTx<HadoopGraph> buildGraknTxFromTinkerGraph(HadoopGraph graph) {
        throw new UnsupportedOperationException(ErrorMessage.CANNOT_PRODUCE_TX.getMessage(HadoopGraph.class.getName()));
    }

    @Override
    protected HadoopGraph buildTinkerPopGraph(boolean batchLoading) {
        //Load Defaults
        TxFactoryJanus.getDefaultProperties().forEach((key, value) -> {
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
