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

package io.mindmaps.core.implementation;


import io.mindmaps.constants.ErrorMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * A thread bound mindmaps transaction
 */
public class MindmapsTinkerTransaction extends AbstractMindmapsTransaction {
    private MindmapsTinkerGraph rootGraph;

    public MindmapsTinkerTransaction(MindmapsTinkerGraph graph) {
        super(graph.getGraph(), graph.isBatchLoadingEnabled());
        this.rootGraph = graph;
    }

    /**
     *
     * @return the root mindmaps graph of the transaction
     */
    @Override
    public AbstractMindmapsGraph getRootGraph() {
        return rootGraph;
    }

    @Override
    protected Graph getNewTransaction() {
        return getRootGraph().getGraph();
    }

    /**
     * Tinker graph is in memory and does not abstract away commits
     */
    @Override
    protected void persistGraph() {
        LOG.warn(ErrorMessage.TINKERGRAPH_WARNING.getMessage());
    }
}
