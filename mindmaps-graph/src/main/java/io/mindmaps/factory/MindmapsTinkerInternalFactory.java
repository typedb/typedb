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

package io.mindmaps.factory;

import io.mindmaps.graph.internal.MindmapsTinkerGraph;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A graph factory which provides a mindmaps graph with a tinker graph backend.
 */
class MindmapsTinkerInternalFactory extends AbstractMindmapsInternalFactory<MindmapsTinkerGraph, TinkerGraph> {
    private final Logger LOG = LoggerFactory.getLogger(MindmapsTinkerInternalFactory.class);

    MindmapsTinkerInternalFactory(String keyspace, String engineUrl, String config){
        super(keyspace, engineUrl, config);
    }

    @Override
    boolean isClosed(TinkerGraph innerGraph) {
        return !innerGraph.traversal().V().has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), Schema.MetaType.ENTITY_TYPE.getId()).hasNext();
    }

    @Override
    MindmapsTinkerGraph buildMindmapsGraphFromTinker(TinkerGraph graph, boolean batchLoading) {
        return new MindmapsTinkerGraph(getTinkerPopGraph(graph, batchLoading), super.keyspace, batchLoading);
    }

    @Override
    TinkerGraph buildTinkerPopGraph(boolean batchLoading) {
        LOG.warn("In memory Tinkergraph ignores the address [" + super.engineUrl + "], " +
                 "the config path [" + super.config + "], and " +
                 "the batch loading [" + batchLoading + "] parameters");
        return TinkerGraph.open();
    }
}
