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

import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class MindmapsTitanTransaction extends AbstractMindmapsTransaction {
    private MindmapsTitanGraph rootGraph;

    public MindmapsTitanTransaction(MindmapsTitanGraph graph) {
        super(graph.getGraph().newTransaction(), graph.isBatchLoadingEnabled());
        rootGraph = graph;
    }

    @Override
    public AbstractMindmapsGraph getRootGraph() {
        return rootGraph;
    }

    @Override
    protected Graph getNewTransaction() {
        AbstractMindmapsGraph mg = getRootGraph();
        return ((TitanGraph) mg.getGraph()).newTransaction();
    }
}
