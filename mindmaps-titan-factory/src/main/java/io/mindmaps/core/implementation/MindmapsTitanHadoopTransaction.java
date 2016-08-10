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

public class MindmapsTitanHadoopTransaction extends MindmapsTransactionImpl {
    private MindmapsTitanHadoopGraph rootGraph;

    public MindmapsTitanHadoopTransaction(MindmapsTitanHadoopGraph graph) {
        super(graph.getGraph(), graph.isBatchLoadingEnabled());
        rootGraph = graph;
    }

    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();
        LOG.info("Graph is valid. Committing graph . . . ");
        getTinkerPopGraph().tx().commit();
        getTransaction().clearTransaction();
        LOG.info("Graph committed.");
    }

    @Override
    public void refresh() throws Exception {
        throw new UnsupportedOperationException(ErrorMessage.NOT_SUPPORTED.getMessage("Titan Hadoop"));
    }

    @Override
    public void close() throws Exception {
        setTinkerPopGraph(null);
    }

    @Override
    public MindmapsGraphImpl getRootGraph() {
        return rootGraph;
    }
}
