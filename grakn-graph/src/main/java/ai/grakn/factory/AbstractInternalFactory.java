/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.factory;

import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.annotation.CheckReturnValue;
import java.util.Properties;

import static javax.annotation.meta.When.MAYBE;

/**
 * <p>
 *     Defines the construction of Grakn Graphs
 * <p/>
 *
 * <p>
 *     Defines the abstract construction of Grakn graphs on top of Tinkerpop Graphs.
 *     For this factory to function a vendor specific implementation of a graph extending
 *     {@link AbstractGraknGraph} must be provided. This must be provided with a matching TinkerPop {@link Graph}
 *     which is wrapped within the Grakn Graph
 * </p>
 *
 * @author fppt
 *
 * @param <M> A Graph Graph extending {@link AbstractGraknGraph} and wrapping a Tinkerpop Graph
 * @param <G> A vendor implementation of a Tinkerpop {@link Graph}
 */
abstract class AbstractInternalFactory<M extends AbstractGraknGraph<G>, G extends Graph> implements InternalFactory<M, G> {

    protected final String keyspace;
    protected final String engineUrl;
    protected final Properties properties;

    protected M graknGraph = null;
    private M batchLoadingGraknGraph = null;

    protected G graph = null;
    private G batchLoadingGraph = null;

    private Boolean lastGraphBuiltBatchLoading = null;
    
    private SystemKeyspace<M, G> systemKeyspace;

    AbstractInternalFactory(String keyspace, String engineUrl, Properties properties){
        if(keyspace == null) {
            throw new GraphRuntimeException(ErrorMessage.NULL_VALUE.getMessage("keyspace"));
        }

        this.keyspace = keyspace.toLowerCase();
        this.engineUrl = engineUrl;
        this.properties = properties;

        if(!keyspace.equals(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
            systemKeyspace = new SystemKeyspace<>(getSystemFactory());
        }
    }

    InternalFactory<M, G> getSystemFactory(){
        //noinspection unchecked
        return FactoryBuilder.getGraknGraphFactory(this.getClass().getName(), SystemKeyspace.SYSTEM_GRAPH_NAME, engineUrl, properties);
    }

    abstract boolean isClosed(G innerGraph);

    abstract M buildGraknGraphFromTinker(G graph, boolean batchLoading);

    abstract G buildTinkerPopGraph(boolean batchLoading);

    @Override
    public synchronized M getGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingGraknGraph = getGraph(batchLoadingGraknGraph, true);
            lastGraphBuiltBatchLoading = true;
            return batchLoadingGraknGraph;
        } else {
            graknGraph = getGraph(graknGraph, false);
            lastGraphBuiltBatchLoading = false;
            return graknGraph;
        }
    }
    protected M getGraph(M graknGraph, boolean batchLoading){
        //This checks if the previous graph built with this factory is the same as the one we trying to build now.
        if(lastGraphBuiltBatchLoading != null && lastGraphBuiltBatchLoading != batchLoading && graknGraph != null){
            //This then checks if the previous graph built has undergone a commit
            boolean hasCommitted;
            if(lastGraphBuiltBatchLoading) {
                hasCommitted = batchLoadingGraknGraph.hasCommitted();
            } else {
                hasCommitted = graknGraph.hasCommitted();
            }

            //Closes the graph to force a full refresh if the other graph has committed
            if(hasCommitted){
                try {
                    graknGraph.finaliseClose(graknGraph::closePermanent, ErrorMessage.CLOSED_FACTORY.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if(graknGraph == null){
            graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
            if (!SystemKeyspace.SYSTEM_GRAPH_NAME.equalsIgnoreCase(this.keyspace)) {
                systemKeyspace.keyspaceOpened(this.keyspace);
            }
        } else {
            if(graknGraph.isClosed()){
                graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
            } else {
                //This check exists because the innerGraph could be closed while the grakn graph is still flagged as open.
                G innerGraph = graknGraph.getTinkerPopGraph();
                synchronized (innerGraph){
                    if(isClosed(innerGraph)){
                        graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading), batchLoading);
                    } else {
                        getGraphWithNewTransaction(graknGraph.getTinkerPopGraph());
                    }
                }
            }
        }

        return graknGraph;
    }

    @Override
    public synchronized G getTinkerPopGraph(boolean batchLoading){
        if(batchLoading){
            batchLoadingGraph = getTinkerPopGraph(batchLoadingGraph, true);
            return batchLoadingGraph;
        } else {
            graph = getTinkerPopGraph(graph, false);
            return graph;
        }
    }
    protected G getTinkerPopGraph(G graph, boolean batchLoading){
        if(graph == null){
            return getGraphWithNewTransaction(buildTinkerPopGraph(batchLoading));
        }

        synchronized (graph){ //Block here because list of open transactions is not thread safe
            if(isClosed(graph)){
                return getGraphWithNewTransaction(buildTinkerPopGraph(batchLoading));
            } else {
                return getGraphWithNewTransaction(graph);
            }
        }
    }

    @CheckReturnValue(when=MAYBE)
    protected abstract G getGraphWithNewTransaction(G graph);

}
