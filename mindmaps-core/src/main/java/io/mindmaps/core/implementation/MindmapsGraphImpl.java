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

public abstract class MindmapsGraphImpl implements MindmapsGraph {
    protected final Logger LOG = LoggerFactory.getLogger(MindmapsGraphImpl.class);
    private boolean batchLoading;
    private Graph graph;

    public MindmapsGraphImpl(Graph graph){
        this.graph = graph;
        checkMetaOntology((MindmapsTransactionImpl) newTransaction());
    }

    @Override
    public abstract MindmapsTransaction newTransaction();

    @Override
    public void enableBatchLoading() {
        batchLoading = true;
    }

    @Override
    public void disableBatchLoading() {
        batchLoading = false;
    }

    @Override
    public boolean isBatchLoadingEnabled(){
        return batchLoading;
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    private void checkMetaOntology(MindmapsTransactionImpl mindmapsTransaction){
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
