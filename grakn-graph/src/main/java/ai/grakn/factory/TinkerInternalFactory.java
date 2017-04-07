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

package ai.grakn.factory;

import ai.grakn.graph.internal.GraknTinkerGraph;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Properties;


/**
 * <p>
 *     A Grakn Graph on top of {@link TinkerGraph}
 * </p>
 *
 * <p>
 *     This produces an in memory grakn graph on top of {@link TinkerGraph}.
 *     The base construction process defined by {@link AbstractInternalFactory} ensures the graph factories are singletons.
 * </p>
 *
 * @author fppt
 */
class TinkerInternalFactory extends AbstractInternalFactory<GraknTinkerGraph, TinkerGraph> {

    TinkerInternalFactory(String keyspace, String engineUrl, Properties properties){
        super(keyspace, engineUrl, properties);
    }

    boolean isClosed(TinkerGraph innerGraph) {
        return !innerGraph.traversal().V().has(Schema.ConceptProperty.TYPE_LABEL.name(), Schema.MetaSchema.ENTITY.getLabel().getValue()).hasNext();
    }

    @Override
    GraknTinkerGraph buildGraknGraphFromTinker(TinkerGraph graph, boolean batchLoading) {
        return new GraknTinkerGraph(graph, super.keyspace, super.engineUrl, batchLoading);
    }

    @Override
    TinkerGraph buildTinkerPopGraph(boolean batchLoading) {
        return TinkerGraph.open();
    }

    @Override
    protected TinkerGraph getTinkerPopGraph(TinkerGraph graph, boolean batchLoading){
        if(super.graph == null || isClosed(super.graph)){
            super.graph = buildTinkerPopGraph(batchLoading);
        }
        return super.graph;
    }

    @Override
    protected TinkerGraph getGraphWithNewTransaction(TinkerGraph graph, boolean batchLoading) {
        return graph;
    }
}
