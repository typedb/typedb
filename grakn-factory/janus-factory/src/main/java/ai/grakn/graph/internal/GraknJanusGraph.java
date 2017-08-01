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

package ai.grakn.graph.internal;

import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graph.internal.concept.ConceptImpl;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.Properties;
import java.util.function.Supplier;

/**
 * <p>
 *     A Grakn Graph using {@link JanusGraph} as a vendor backend.
 * </p>
 *
 * <p>
 *     Wraps up a {@link JanusGraph} as a method of storing the Grakn Graph object Model.
 *     With this vendor some issues to be aware of:
 *     1. Whenever a transaction is closed if none remain open then the connection to the graph is closed permanently.
 *     2. Clearing the graph explicitly closes the connection as well.
 * </p>
 *
 * @author fppt
 */
public class GraknJanusGraph extends AbstractGraknGraph<JanusGraph> {
    public GraknJanusGraph(JanusGraph graph, String name, String engineUrl, Properties properties){
        super(graph, name, engineUrl, properties);
    }

    /**
     * Uses {@link JanusGraphVertex#isModified()}
     *
     * @param concept A concept in the graph
     * @return true if the concept has been modified
     */
    @Override
    public boolean isConceptModified(Concept concept) {
        //TODO: Clean this crap up
        if(concept instanceof ConceptImpl) {
            JanusGraphVertex vertex = (JanusGraphVertex) ((ConceptImpl) concept).vertex().element();
            return vertex.isModified() || vertex.isNew();
        }
        return true;
    }

    @Override
    public void openTransaction(GraknTxType txType){
        super.openTransaction(txType);
        if(getTinkerPopGraph().isOpen() && !getTinkerPopGraph().tx().isOpen()) getTinkerPopGraph().tx().open();
    }

    @Override
    public boolean isSessionClosed() {
        return getTinkerPopGraph().isClosed();
    }

    @Override
    public int numOpenTx() {
        return ((StandardJanusGraph) getTinkerPopGraph()).getOpenTxs();
    }

    @Override
    protected void clearGraph() {
        JanusGraphCleanup.clear(getTinkerPopGraph());
    }

    @Override
    public void commitTransactionInternal(){
        executeLockingMethod(() -> {
            super.commitTransactionInternal();
            return null;
        });
    }

    @Override
    public VertexElement addVertex(Schema.BaseType baseType){
        return executeLockingMethod(() -> super.addVertex(baseType));
    }

    /**
     * Executes a method which has the potential to throw a {@link TemporaryLockingException} or a {@link PermanentLockingException}.
     * If the exception is thrown it is wrapped in a {@link GraknBackendException} so that the transaction can be retried.
     *
     * @param method The locking method to execute
     */
    private <X> X executeLockingMethod(Supplier<X> method){
        try {
            return method.get();
        } catch (JanusGraphException e){
            if(e.isCausedBy(TemporaryLockingException.class) || e.isCausedBy(PermanentLockingException.class)){
                throw TemporaryWriteException.temporaryLock(e);
            } else {
                throw GraknBackendException.unknown(e);
            }
        }
    }

    @Override
    public void validVertex(Vertex vertex) {
        super.validVertex(vertex);

        if(((JanusGraphVertex) vertex).isRemoved()){
            throw new IllegalStateException("The vertex [" + vertex + "] has been removed and is no longer valid");
        }
    }
}
