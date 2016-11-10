/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.factory;

import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

abstract class AbstractInternalFactory<M extends AbstractMindmapsGraph<G>, G extends Graph> implements InternalFactory<M, G> {
    protected final String keyspace;
    protected final String engineUrl;
    protected final String config;

    protected M mindmapsGraph = null;
    private M batchLoadingMindmapsGraph = null;

    protected G graph = null;
    private G batchLoadingGraph = null;

    private Boolean lastGraphBuiltBatchLoading = null;

    AbstractInternalFactory(String keyspace, String engineUrl, String config){
        this.keyspace = keyspace.toLowerCase();
        this.engineUrl = engineUrl;
        this.config = config;
    }

    abstract boolean isClosed(G innerGraph);

    abstract M buildMindmapsGraphFromTinker(G graph, boolean batchLoading);

    abstract G buildTinkerPopGraph();

    @Override
    public synchronized M getGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingMindmapsGraph = getGraph(batchLoadingMindmapsGraph, batchLoadingGraph, batchLoading);
            lastGraphBuiltBatchLoading = true;
            return batchLoadingMindmapsGraph;
        } else {
            mindmapsGraph = getGraph(mindmapsGraph, graph, batchLoading);
            lastGraphBuiltBatchLoading = false;
            return mindmapsGraph;
        }
    }
    protected M getGraph(M mindmapsGraph, G graph, boolean batchLoading){
        //This checks if the previous graph built with this factory is the same as the one we trying to build now.
        if(lastGraphBuiltBatchLoading != null && lastGraphBuiltBatchLoading != batchLoading && mindmapsGraph != null){
            //This then checks if the previous graph built has undergone a commit
            boolean hasCommitted = false;
            if(lastGraphBuiltBatchLoading) {
                hasCommitted = batchLoadingMindmapsGraph.hasCommitted();
            } else {
                hasCommitted = mindmapsGraph.hasCommitted();
            }

            //Closes the graph to force a full refresh if the other graph has committed
            if(hasCommitted){
                try {
                    mindmapsGraph.getTinkerPopGraph().close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if(mindmapsGraph == null || isClosed(mindmapsGraph)){
            mindmapsGraph = buildMindmapsGraphFromTinker(getTinkerPopGraph(graph), batchLoading);
        }
        return mindmapsGraph;
    }

    @Override
    public synchronized G getTinkerPopGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingGraph = getTinkerPopGraph(batchLoadingGraph);
            return batchLoadingGraph;
        } else {
            graph = getTinkerPopGraph(graph);
            return graph;
        }
    }
    protected G getTinkerPopGraph(G graph){
        if(graph == null || isClosed(graph)){
            graph = buildTinkerPopGraph();
        }
        return graph;
    }

    private boolean isClosed(M mindmapsGraph) {
        G innerGraph = mindmapsGraph.getTinkerPopGraph();
        return isClosed(innerGraph);
    }
}
