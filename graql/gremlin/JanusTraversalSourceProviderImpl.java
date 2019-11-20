/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 *
 */

package grakn.core.graql.gremlin;

import grakn.core.kb.graql.gremlin.JanusTraversalSourceProvider;
import grakn.core.kb.server.exception.TransactionException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.janusgraph.core.JanusGraphTransaction;

import javax.annotation.Nullable;

public class JanusTraversalSourceProviderImpl implements JanusTraversalSourceProvider {
    private final JanusGraphTransaction janusGraphTransaction;

    // Thread-local boolean which is set to true in the constructor. Used to check if operating in the same thread
    // object was constructed in reaching across threads in a single threaded janus transaction leads to errors
    private final ThreadLocal<Boolean> createdInCurrentThread = ThreadLocal.withInitial(() -> Boolean.FALSE);

    @Nullable
    private GraphTraversalSource graphTraversalSource = null;

    public JanusTraversalSourceProviderImpl(JanusGraphTransaction janusGraphTransaction) {
        this.janusGraphTransaction = janusGraphTransaction;
        createdInCurrentThread.set(true);
    }

    /**
     * As a janus optimisation, the traversal source should be created once and re-used to spawn new
     * Traversal instances when needed.
     * @return A read-only Tinkerpop traversal for traversing the graph
     */
    @Override
    public GraphTraversalSource getTinkerTraversal() {
        checkThreadLocal();
        if (graphTraversalSource == null) {
            graphTraversalSource = janusGraphTransaction.traversal().withStrategies(ReadOnlyStrategy.instance());
        }
        return graphTraversalSource;
    }

    private void checkThreadLocal() {
        if (!createdInCurrentThread.get()) {
            throw TransactionException.notInOriginatingThread();
        }
    }
}
