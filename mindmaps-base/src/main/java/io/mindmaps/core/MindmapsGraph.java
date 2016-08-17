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

package io.mindmaps.core;

import io.mindmaps.MindmapsTransaction;

/**
 * A mindmaps graph which produces new transactions to work with
 */
public interface MindmapsGraph {
    /**
     *
     * @return the thread bound transaction. If this is called by a different thread a new transaction is created.
     */
    MindmapsTransaction getTransaction();

    /**
     * Closes the graph making it unusable
     */
    void close();

    /**
     * Clears the graph completely. WARNING: This will invalidate any open transactions.
     */
    void clear();

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

    /**
     *
     * @return The name of the graph you are operating on.
     */
    String getName();
}
