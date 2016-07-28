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

package io.mindmaps.core.dao;

import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * A mindmaps graph which produces new transactions to work with
 */
public interface MindmapsGraph {
    /**
     *
     * @return A new transaction with a snapshot of the graph at the time of creation
     */
    MindmapsTransaction newTransaction();

    /**
     * Closes the graph making it unusable
     */
    void close();

    /**
     * Clears the graph completely. WARNING: This will invalidate any open transactions.
     */
    void clear();

    /**
     *
     * @return Returns the underlaying gremlin graph.
     */
    Graph getGraph();

    /**
     * Enables batch loading which skips redundancy checks.
     * With this mode enabled duplicate concepts and relations maybe created.
     * Faster writing at the cost of consistency.
     */
    void enableBatchLoading();

    /**
     * Disables batch loading which prevents the creation of duplicate castings.
     * Immediate constancy at the cost of writing speed.
     */
    void disableBatchLoading();

    /**
     *
     * @return A flag indicating if this transaction is batch loading or not
     */
    boolean isBatchLoadingEnabled();
}
