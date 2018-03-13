/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.annotation.CheckReturnValue;

import static javax.annotation.meta.When.NEVER;

/**
 * <p>
 *     Defines the construction of Grakn Graphs
 * <p/>
 *
 * <p>
 *     Defines the abstract construction of {@link GraknTx}s on top of Tinkerpop Graphs.
 *     For this factory to function a vendor specific implementation of a graph extending
 *     {@link EmbeddedGraknTx} must be provided. This must be provided with a matching TinkerPop {@link Graph}
 *     which is wrapped within the {@link GraknTx}
 * </p>
 *
 * @author fppt
 *
 * @param <M> A {@link GraknTx} extending {@link EmbeddedGraknTx} and wrapping a Tinkerpop Graph
 * @param <G> A vendor implementation of a Tinkerpop {@link Graph}
 */
abstract class TxFactoryAbstract<M extends EmbeddedGraknTx<G>, G extends Graph> implements TxFactory<G> {
    private final EmbeddedGraknSession session;

    private M graknTx = null;
    private M graknTxBatchLoading = null;
    
    G tx = null;
    private G txBatchLoading = null;

    TxFactoryAbstract(EmbeddedGraknSession session){
        this.session = session;
    }

    abstract M buildGraknGraphFromTinker(G graph);

    abstract G buildTinkerPopGraph(boolean batchLoading);

    @Override
    public synchronized M open(GraknTxType txType){
        if(GraknTxType.BATCH.equals(txType)){
            checkOtherGraphOpen(graknTx);
            graknTxBatchLoading = getGraph(graknTxBatchLoading, txType);
            return graknTxBatchLoading;
        } else {
            checkOtherGraphOpen(graknTxBatchLoading);
            graknTx = getGraph(graknTx, txType);
            return graknTx;
        }
    }

    private void checkOtherGraphOpen(GraknTx otherGraph){
        if(otherGraph != null && !otherGraph.isClosed()) throw GraknTxOperationException.transactionOpen(otherGraph);
    }

    private M getGraph(M graknGraph, GraknTxType txType){
        boolean batchLoading = GraknTxType.BATCH.equals(txType);

        if(graknGraph == null){
            graknGraph = buildGraknGraphFromTinker(getTinkerPopGraph(batchLoading));
        } else {
            if(!graknGraph.isClosed()) throw GraknTxOperationException.transactionOpen(graknGraph);

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
            txBatchLoading = getTinkerPopGraph(txBatchLoading, true);
            return txBatchLoading;
        } else {
            tx = getTinkerPopGraph(tx, false);
            return tx;
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

    public EmbeddedGraknSession session(){
        return session;
    }
}
