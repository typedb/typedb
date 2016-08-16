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
import io.mindmaps.constants.RESTUtil;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.MindmapsTransaction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mindmaps graph which produces new transactions to work with
 */
public abstract class AbstractMindmapsGraph<G extends Graph> implements MindmapsGraph {
    protected final Logger LOG = LoggerFactory.getLogger(AbstractMindmapsGraph.class);
    private final String engineUrl;
    private final String graphName;
    private boolean batchLoading;
    private G graph;

    public AbstractMindmapsGraph(G graph, String graphName, String engineUrl){
        this.graph = graph;
        this.graphName = graphName;
        this.engineUrl = engineUrl;
        checkSchema((AbstractMindmapsTransaction) newTransaction());
    }

    public String getCommitLogEndPoint(){
        return getEngineUrl() + RESTUtil.WebPath.COMMIT_LOG_URI + "?" + RESTUtil.Request.GRAPH_NAME_PARAM + "=" + getName();
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
     * @return The name of the graph you are operating on.
     */
    @Override
    public String getName(){
        return graphName;
    }

    /**
     *
     * @return Returns the underlaying gremlin graph.
     */
    public G getGraph() {
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
    private void checkSchema(AbstractMindmapsTransaction mindmapsTransaction){
        if(mindmapsTransaction.isMetaOntologyNotInitialised()){
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

    /**
     * Closes the graph making it unusable
     */
    @Override
    public void close() {
        try {
            getGraph().close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
