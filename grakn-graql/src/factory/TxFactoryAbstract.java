/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.factory;

import grakn.core.GraknTx;
import grakn.core.GraknTxType;
import grakn.core.exception.GraknTxOperationException;
import grakn.core.kb.internal.EmbeddedGraknTx;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

import static javax.annotation.meta.When.NEVER;

/**
 * <p>
 * Defines the abstract construction of {@link GraknTx}s on top of Tinkerpop Graphs.
 * For this factory to function a vendor specific implementation of a graph extending
 * {@link EmbeddedGraknTx} must be provided. This must be provided with a matching TinkerPop {@link Graph}
 * which is wrapped within the {@link GraknTx}
 * </p>
 *
 * @param <Tx> A {@link GraknTx} extending {@link EmbeddedGraknTx} and wrapping a Tinkerpop Graph
 * @param <G>  A vendor implementation of a Tinkerpop {@link Graph}
 * @author fppt
 */
public abstract class TxFactoryAbstract<Tx extends EmbeddedGraknTx<G>, G extends Graph> implements TxFactory<G> {
    private final EmbeddedGraknSession session;

    protected final GraphWithTx batchTinkerPopGraphWithTx = new GraphWithTx(true);
    protected final GraphWithTx tinkerPopGraphWithTx = new GraphWithTx(false);

    protected TxFactoryAbstract(EmbeddedGraknSession session) {
        this.session = session;
    }

    protected abstract Tx buildGraknTxFromTinkerGraph(G graph);

    protected abstract G buildTinkerPopGraph(boolean batchLoading);

    @Override
    final public synchronized Tx open(GraknTxType txType) {
        if (GraknTxType.BATCH.equals(txType)) {
            tinkerPopGraphWithTx.checkTxIsOpen();
            return batchTinkerPopGraphWithTx.openTx(txType);
        } else {
            // Check there is no batch loading Tx open
            batchTinkerPopGraphWithTx.checkTxIsOpen();
            return tinkerPopGraphWithTx.openTx(txType);
        }
    }


    @Override
    final public synchronized G getTinkerPopGraph(boolean batchLoading) {
        if (batchLoading) {
            return batchTinkerPopGraphWithTx.getTinkerPopGraph();
        } else {
            return tinkerPopGraphWithTx.getTinkerPopGraph();
        }
    }


    @CheckReturnValue(when = NEVER)
    protected abstract G getGraphWithNewTransaction(G graph, boolean batchloading);

    final public EmbeddedGraknSession session() {
        return session;
    }


    /**
     * Helper class representing a TinkerPop graph that can be open with batchLoading enabled or disabled
     */

    class GraphWithTx {
        @Nullable
        private Tx graknTx = null;
        private G graph = null;
        private final boolean batchLoading;

        public GraphWithTx(boolean batchLoading) {
            this.batchLoading = batchLoading;
        }

        public Tx openTx(GraknTxType txType) {
            initialiseGraknTx();
            graknTx.openTransaction(txType);
            return graknTx;
        }

        private void initialiseGraknTx() {
            // If transaction is already open throw exception
            if (graknTx != null && !graknTx.isClosed()) throw GraknTxOperationException.transactionOpen(graknTx);

            // Create new transaction from a Tinker graph if tx is null or s closed
            if (graknTx == null || graknTx.isTinkerPopGraphClosed()) {
                graknTx = buildGraknTxFromTinkerGraph(getTinkerPopGraph());
            }
        }

        protected G getTinkerPopGraph() {
            if (graph == null) {
                graph = buildTinkerPopGraph(batchLoading);
            } else {
                graph = getGraphWithNewTransaction(graph, batchLoading);
            }
            return graph;
        }

        public void checkTxIsOpen() {
            if (graknTx != null && !graknTx.isClosed()) throw GraknTxOperationException.transactionOpen(graknTx);
        }

    }
}
