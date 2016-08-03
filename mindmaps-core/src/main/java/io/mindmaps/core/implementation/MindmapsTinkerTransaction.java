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

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.MindmapsValidationException;

/**
 * A thread bound mindmaps transaction
 */
public class MindmapsTinkerTransaction extends MindmapsTransactionImpl {
    MindmapsTinkerGraph rootGraph;

    public MindmapsTinkerTransaction(MindmapsTinkerGraph graph) {
        super(graph.getGraph(), graph.isBatchLoadingEnabled());
        this.rootGraph = graph;
    }

    /**
     * Validates and attempts to commit the graph. An exception is thrown if validation fails or if the graph cannot be persisted due to an underlying database issue.
     * @throws MindmapsValidationException is thrown when a structural validation fails.
     */
    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();
        getTransaction().clearTransaction();
        LOG.warn(ErrorMessage.TINKERGRAPH_WARNING.getMessage());
        getTransaction().clearTransaction();
    }

    /**
     * Resets the current transaction without commiting.
     * @throws UnsupportedOperationException due to tinkergraph not supporting refresh of transactions
     */
    @Override
    public void refresh() throws Exception {
        throw new UnsupportedOperationException(ErrorMessage.NOT_SUPPORTED.getMessage("Tinkergraph"));
    }

    /**
     * Closes the current transaction rendering it unusable.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        getTransaction().clearTransaction();
        setTinkerPopGraph(null);
    }

    /**
     *
     * @return the root mindmaps graph of the transaction
     */
    @Override
    public MindmapsGraph getRootGraph() {
        return rootGraph;
    }
}
