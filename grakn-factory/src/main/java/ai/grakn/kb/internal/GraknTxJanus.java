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

package ai.grakn.kb.internal;

/*-
 * #%L
 * grakn-factory
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.function.Supplier;

/**
 * <p>
 *     A {@link GraknTx} using {@link JanusGraph} as a vendor backend.
 * </p>
 *
 * <p>
 *     Wraps up a {@link JanusGraph} as a method of storing the {@link GraknTx} object Model.
 *     With this vendor some issues to be aware of:
 *     1. Whenever a transaction is closed if none remain open then the connection to the graph is closed permanently.
 *     2. Clearing the graph explicitly closes the connection as well.
 * </p>
 *
 * @author fppt
 */
public class GraknTxJanus extends EmbeddedGraknTx<JanusGraph> {
    public GraknTxJanus(EmbeddedGraknSession session, JanusGraph graph){
        super(session, graph);
    }

    @Override
    public void openTransaction(GraknTxType txType){
        super.openTransaction(txType);
        if(getTinkerPopGraph().isOpen() && !getTinkerPopGraph().tx().isOpen()) getTinkerPopGraph().tx().open();
    }

    @Override
    public boolean isTinkerPopGraphClosed() {
        return getTinkerPopGraph().isClosed();
    }

    @Override
    public int numOpenTx() {
        return ((StandardJanusGraph) getTinkerPopGraph()).getOpenTransactions().size();
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
    public VertexElement addVertexElement(Schema.BaseType baseType, ConceptId... conceptIds){
        return executeLockingMethod(() -> super.addVertexElement(baseType, conceptIds));
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
    public boolean isValidElement(Element element) {
        return super.isValidElement(element) && !((JanusGraphElement) element).isRemoved();
    }
}
