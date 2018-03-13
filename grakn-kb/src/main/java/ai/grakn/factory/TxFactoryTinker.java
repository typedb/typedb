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

import ai.grakn.GraknTx;
import ai.grakn.kb.internal.GraknTxTinker;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;


/**
 * <p>
 *     A {@link GraknTx} on top of {@link TinkerGraph}
 * </p>
 *
 * <p>
 *     This produces an in memory grakn graph on top of {@link TinkerGraph}.
 *     The base construction process defined by {@link TxFactoryAbstract} ensures the graph factories are singletons.
 * </p>
 *
 * @author fppt
 */
public class TxFactoryTinker extends TxFactoryAbstract<GraknTxTinker, TinkerGraph> {

    TxFactoryTinker(EmbeddedGraknSession session){
        super(session);
    }

    private boolean isClosed(TinkerGraph innerGraph) {
        return !innerGraph.traversal().V().has(Schema.VertexProperty.SCHEMA_LABEL.name(), Schema.MetaSchema.ENTITY.getLabel().getValue()).hasNext();
    }

    @Override
    GraknTxTinker buildGraknGraphFromTinker(TinkerGraph graph) {
        return new GraknTxTinker(session(), graph);
    }

    @Override
    TinkerGraph buildTinkerPopGraph(boolean batchLoading) {
        return TinkerGraph.open();
    }

    @Override
    protected TinkerGraph getTinkerPopGraph(TinkerGraph graph, boolean batchLoading){
        if(super.tx == null || isClosed(super.tx)){
            super.tx = buildTinkerPopGraph(batchLoading);
        }
        return super.tx;
    }

    @Override
    protected TinkerGraph getGraphWithNewTransaction(TinkerGraph graph, boolean batchLoading) {
        return graph;
    }
}
