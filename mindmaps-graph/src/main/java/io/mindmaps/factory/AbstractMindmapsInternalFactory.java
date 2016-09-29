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

abstract class AbstractMindmapsInternalFactory<M extends AbstractMindmapsGraph<G>, G extends Graph> implements MindmapsInternalFactory<M, G> {
    protected final String keyspace;
    protected final String engineUrl;
    protected final String config;

    private M mindmapsGraph = null;
    private M batchLoadingMindmapsGraph = null;

    private G graph = null;
    private G batchLoadingGraph = null;

    AbstractMindmapsInternalFactory(String keyspace, String engineUrl, String config){
        this.keyspace = keyspace.toLowerCase();
        this.engineUrl = engineUrl;
        this.config = config;
    }

    abstract boolean isClosed(G innerGraph);

    abstract M buildMindmapsGraphFromTinker(G graph, boolean batchLoading);

    abstract G buildTinkerPopGraph();

    @Override
    public M getGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingMindmapsGraph = getGraph(batchLoadingMindmapsGraph, batchLoadingGraph, batchLoading);
            return batchLoadingMindmapsGraph;
        } else {
            mindmapsGraph = getGraph(mindmapsGraph, graph, batchLoading);
            return mindmapsGraph;
        }
    }
    private M getGraph(M mindmapsGraph, G graph, boolean batchLoading){
        if(mindmapsGraph == null || isClosed(mindmapsGraph)){
            mindmapsGraph = buildMindmapsGraphFromTinker(getTinkerPopGraph(graph), batchLoading);
        }
        return mindmapsGraph;
    }

    @Override
    public G getTinkerPopGraph(boolean batchLoading){
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
