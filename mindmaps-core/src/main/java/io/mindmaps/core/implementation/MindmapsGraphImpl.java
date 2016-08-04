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
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mindmaps graph which produces new transactions to work with
 */
public abstract class MindmapsGraphImpl implements MindmapsGraph {
    protected final Logger LOG = LoggerFactory.getLogger(MindmapsGraphImpl.class);
    private final String engineUrl;
    private boolean batchLoading;
    private Graph graph;

    public MindmapsGraphImpl(Graph graph, String engineUrl){
        this.graph = graph;
        this.engineUrl = engineUrl;
        checkSchema((MindmapsTransactionImpl) newTransaction());
    }

    /**
     *
     * @return A new transaction with a snapshot of the graph at the time of creation
     */
    @Override
    public abstract MindmapsTransaction newTransaction();

    /**
     * Enables batch loading which skips redundancy checks.
     * With this mode enabled duplicate concepts and relations maybe created.
     * Faster writing at the cost of consistency.
     */
    @Override
    public void enableBatchLoading() {
        batchLoading = true;
    }

    /**
     * Disables batch loading which prevents the creation of duplicate castings.
     * Immediate constancy at the cost of writing speed.
     */
    @Override
    public void disableBatchLoading() {
        batchLoading = false;
    }

    /**
     *
     * @return A flag indicating if this transaction is batch loading or not
     */
    @Override
    public boolean isBatchLoadingEnabled(){
        return batchLoading;
    }

    /**
     *
     * @return Returns the underlaying gremlin graph.
     */
    @Override
    public Graph getGraph() {
        return graph;
    }

    /**
     *
     * @return Engine's url
     */
    public String getEngineUrl(){
        return engineUrl;
    }

    /**
     * Checks if the schema exists if not it creates and commits it.
     * @param mindmapsTransaction A transaction to use to check the schema
     */
    private void checkSchema(MindmapsTransactionImpl mindmapsTransaction){
        if(!mindmapsTransaction.isMetaOntologyInitialised()){
            mindmapsTransaction.initialiseMetaConcepts();
            try {
                mindmapsTransaction.commit();
            } catch (MindmapsValidationException e) {
                LOG.error(ErrorMessage.CREATING_ONTOLOGY_ERROR.getMessage(e.getMessage()), e);
            }
        } else {
            try {
                mindmapsTransaction.close();
            } catch (Exception e) {
                LOG.error(ErrorMessage.CREATING_ONTOLOGY_ERROR.getMessage(e.getMessage()), e);
            }
        }
    }

}
