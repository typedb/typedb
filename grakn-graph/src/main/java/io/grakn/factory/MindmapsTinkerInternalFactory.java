/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.factory;

import io.grakn.graph.internal.GraknTinkerGraph;
import io.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A graph factory which provides a grakn graph with a tinker graph backend.
 */
class MindmapsTinkerInternalFactory extends AbstractMindmapsInternalFactory<GraknTinkerGraph, TinkerGraph> {
    private final Logger LOG = LoggerFactory.getLogger(MindmapsTinkerInternalFactory.class);

    MindmapsTinkerInternalFactory(String keyspace, String engineUrl, String config){
        super(keyspace, engineUrl, config);
    }

    @Override
    boolean isClosed(TinkerGraph innerGraph) {
        return !innerGraph.traversal().V().has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), Schema.MetaSchema.ENTITY_TYPE.getId()).hasNext();
    }

    @Override
    GraknTinkerGraph buildMindmapsGraphFromTinker(TinkerGraph graph, boolean batchLoading) {
        return new GraknTinkerGraph(graph, super.keyspace, super.engineUrl, batchLoading);
    }

    @Override
    TinkerGraph buildTinkerPopGraph() {
        LOG.warn("In memory Tinkergraph ignores the address [" + super.engineUrl + "] and " +
                 "the config path [" + super.config + "]");
        return TinkerGraph.open();
    }

    @Override
    protected TinkerGraph getTinkerPopGraph(TinkerGraph graph){
        if(super.graph == null || isClosed(super.graph)){
            super.graph = buildTinkerPopGraph();
        }
        return super.graph;
    }
}
