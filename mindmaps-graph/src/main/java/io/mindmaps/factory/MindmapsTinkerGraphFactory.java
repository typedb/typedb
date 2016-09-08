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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A graph factory which provides a mindmaps graph with a tinker graph backend.
 */
class MindmapsTinkerGraphFactory extends AbstractMindmapsGraphFactory<MindmapsTinkerGraph, TinkerGraph> {
    private final Logger LOG = LoggerFactory.getLogger(MindmapsTinkerGraphFactory.class);

    MindmapsTinkerGraphFactory(){
        super();
    }

    @Override
    boolean isClosed(TinkerGraph innerGraph) {
        return false;
    }

    @Override
    MindmapsTinkerGraph buildMindmapsGraphFromTinker(TinkerGraph graph, String name, String address, boolean batchLoading) {
        return new MindmapsTinkerGraph(graph, name, batchLoading);
    }

    @Override
    TinkerGraph buildTinkerPopGraph(String name, String address, String pathToConfig) {
        LOG.warn("In memory Tinkergraph ignores the address [" + address + "] and config path [" + pathToConfig + "]parameters");
        return TinkerGraph.open();
    }
}
