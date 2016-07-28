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
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.exceptions.MindmapsValidationException;

public class MindmapsTitanTransaction extends MindmapsTransactionImpl {
    private MindmapsTitanGraph rootGraph;

    public MindmapsTitanTransaction(MindmapsTitanGraph graph) {
        super(((TitanGraph) graph.getGraph()).newTransaction(), graph.isBatchLoadingEnabled());
        rootGraph = graph;
    }

    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();
        LOG.info("Graph is valid. Committing graph . . . ");
        getTinkerPopGraph().tx().commit();
        try {
            refreshTransaction();
        } catch (Exception e) {
            LOG.error("Failed to create new transaction after committing", e);
            e.printStackTrace();
        }
        LOG.info("Graph committed.");
    }

    @Override
    public void refresh() throws Exception {
        close();
        refreshTransaction();
    }

    @Override
    public void close() throws Exception {
        getTinkerPopGraph().close();
        setTinkerPopGraph(null);
    }

    private void refreshTransaction() throws Exception {
        getTinkerPopGraph().close();
        setTinkerPopGraph(((TitanGraph) rootGraph.getGraph()).newTransaction());
        getTransaction().clearTransaction();
    }

    @Override
    public MindmapsGraph getRootGraph() {
        return rootGraph;
    }
}
