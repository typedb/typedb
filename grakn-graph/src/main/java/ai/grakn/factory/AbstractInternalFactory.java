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

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.annotation.CheckReturnValue;
import java.util.Objects;
import java.util.Properties;

import static javax.annotation.meta.When.NEVER;

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
abstract class AbstractInternalFactory<M extends AbstractGraknGraph<G>, G extends Graph> implements InternalFactory<G> {

    protected final String keyspace;
    protected final String engineUrl;
    protected final Properties properties;

    private M graknGraph = null;
    private M batchLoadingGraknGraph = null;
    
    G graph = null;
    private G batchLoadingGraph = null;

    AbstractInternalFactory(String keyspace, String engineUrl, Properties properties){
        Objects.requireNonNull(keyspace);

        this.keyspace = keyspace.toLowerCase();
        this.engineUrl = engineUrl;
        this.properties = properties;
    }

    abstract M buildGraknGraphFromTinker(G graph);

    abstract G buildTinkerPopGraph(boolean batchLoading);

    @Override
    public synchronized M open(GraknTxType txType){
        if(GraknTxType.BATCH.equals(txType)){
            checkOtherGraphOpen(graknGraph);
            batchLoadingGraknGraph = getGraph(batchLoadingGraknGraph, txType);
            return batchLoadingGraknGraph;
        } else {
            checkOtherGraphOpen(batchLoadingGraknGraph);
            graknGraph = getGraph(graknGraph, txType);
            return graknGraph;
        }
    }

    private void checkOtherGraphOpen(GraknGraph otherGraph){
        if(otherGraph != null && !otherGraph.isClosed()) throw GraphOperationException.transactionOpen(otherGraph);
    }

    private M getGraph(M graknGraph, GraknTxType txType){
        boolean batchLoading = GraknTxType.BATCH.equals(txType);

        if(graknGraph == null){
            graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading));
        } else {
            if(!graknGraph.isClosed()) throw GraphOperationException.transactionOpen(graknGraph);

            if(graknGraph.isSessionClosed()){
                graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading));
            }
        }
        graknGraph.openTransaction(txType);
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
    G getTinkerPopGraph(G graph, boolean batchLoading){
        if(graph == null){
            return buildTinkerPopGraph(batchLoading);
        }

        return getGraphWithNewTransaction(graph, batchLoading);
    }

    @CheckReturnValue(when=NEVER)
    protected abstract G getGraphWithNewTransaction(G graph, boolean batchloading);

}
