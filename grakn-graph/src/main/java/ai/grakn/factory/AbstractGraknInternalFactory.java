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

package ai.grakn.factory;

import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.AbstractGraknGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

abstract class AbstractGraknInternalFactory<M extends AbstractGraknGraph<G>, G extends Graph> implements GraknInternalFactory<M, G> {
    protected final String keyspace;
    protected final String engineUrl;
    protected final String config;

    protected M graknGraph = null;
    private M batchLoadingGraknGraph = null;

    protected G graph = null;
    private G batchLoadingGraph = null;

    AbstractGraknInternalFactory(String keyspace, String engineUrl, String config){
        this.keyspace = keyspace.toLowerCase();
        this.engineUrl = engineUrl;
        this.config = config;
    }

    abstract boolean isClosed(G innerGraph);

    abstract M buildGraknGraphFromTinker(G graph, boolean batchLoading);

    abstract G buildTinkerPopGraph();

    @Override
    public synchronized M getGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingGraknGraph = getGraph(batchLoadingGraknGraph, batchLoadingGraph, batchLoading);
            return batchLoadingGraknGraph;
        } else {
            graknGraph = getGraph(graknGraph, graph, batchLoading);
            return graknGraph;
        }
    }
    protected M getGraph(M graknGraph, G graph, boolean batchLoading){
        if(graknGraph == null || isClosed(graknGraph)){
            graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(graph), batchLoading);
        }
        return graknGraph;
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

    private boolean isClosed(M graknGraph) {
        G innerGraph = graknGraph.getTinkerPopGraph();
        return isClosed(innerGraph);
    }
}
