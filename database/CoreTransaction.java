/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.common.StatisticsKey;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.logic.LogicCache;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.query.QueryManager;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typedb.core.traversal.TraversalCache;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.rocksdb.RocksDBException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.common.collection.Collections.hasIntersection;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.collection.ByteArray.encodeLongSet;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_COMMIT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CONSISTENCY_DELETE_MODIFY_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CONSISTENCY_EXCLUSIVE_CREATE_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CONSISTENCY_MODIFY_DELETE_VIOLATION;

public abstract class CoreTransaction implements TypeDB.Transaction {

    protected final CoreSession session;
    protected final Context.Transaction context;
    protected GraphManager graphMgr;
    protected ConceptManager conceptMgr;
    protected AtomicBoolean isOpen;
    protected LogicManager logicMgr;
    TraversalEngine traversalEng;
    Reasoner reasoner;
    QueryManager queryMgr;

    private CoreTransaction(CoreSession session, Arguments.Transaction.Type type, Options.Transaction options) {
        this.session = session;
        this.context = new Context.Transaction(session.context(), options).type(type);
    }

    void initialise(GraphManager graphMgr, TraversalCache traversalCache, LogicCache logicCache) {
        traversalEng = new TraversalEngine(graphMgr, traversalCache);
        conceptMgr = new ConceptManager(graphMgr);
        logicMgr = new LogicManager(graphMgr, conceptMgr, traversalEng, logicCache);
        reasoner = new Reasoner(conceptMgr, logicMgr, traversalEng, context);
        queryMgr = new QueryManager(conceptMgr, logicMgr, reasoner, context);
        isOpen = new AtomicBoolean(true);
    }

    public Context.Transaction context() {
        return context;
    }

    public TraversalEngine traversal() {
        return traversalEng;
    }

    @Override
    public Arguments.Transaction.Type type() {
        return context.transactionType();
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public QueryManager query() {
        if (!isOpen.get()) throw TypeDBException.of(TRANSACTION_CLOSED);
        return queryMgr;
    }

    @Override
    public ConceptManager concepts() {
        if (!isOpen.get()) throw TypeDBException.of(TRANSACTION_CLOSED);
        return conceptMgr;
    }

    @Override
    public LogicManager logic() {
        if (!isOpen.get()) throw TypeDBException.of(TRANSACTION_CLOSED);
        return logicMgr;
    }

    public Reasoner reasoner() {
        if (!isOpen.get()) throw TypeDBException.of(TRANSACTION_CLOSED);
        return reasoner;
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            // TODO: this should call `doClose()` which is then also called from `commit()`
            closeResources();
            notifyClosed();
        }
    }

    void notifyClosed() {
        session.closed(this);
    }

    protected abstract void closeResources();

    boolean isSchema() {
        return false;
    }

    boolean isData() {
        return false;
    }

    Schema asSchema() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Schema.class));
    }

    Data asData() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Data.class));
    }

    public static class Schema extends CoreTransaction {

        protected final RocksStorage.Schema schemaStorage;
        protected final RocksStorage.Data dataStorage;

        protected Schema(CoreSession.Schema session, Arguments.Transaction.Type type,
                         Options.Transaction options, Factory.Storage storageFactory) {
            super(session, type, options);

            schemaStorage = storageFactory.storageSchema(session.database(), this);
            TypeGraph typeGraph = new TypeGraph(schemaStorage, type().isRead());

            dataStorage = storageFactory.storageData(session.database(), this);
            ThingGraph thingGraph = new ThingGraph(dataStorage, typeGraph);

            graphMgr = new GraphManager(typeGraph, thingGraph);
            initialise(graphMgr, new TraversalCache(), new LogicCache());
        }

        @Override
        boolean isSchema() {
            return true;
        }

        @Override
        Schema asSchema() {
            return this;
        }

        TypeGraph graph() {
            return graphMgr.schema();
        }

        RocksStorage.Schema schemaStorage() {
            if (!isOpen.get()) throw TypeDBException.of(TRANSACTION_CLOSED);
            return schemaStorage;
        }

        RocksStorage.Data dataStorage() {
            if (!isOpen.get()) throw TypeDBException.of(TRANSACTION_CLOSED);
            return dataStorage;
        }

        @Override
        public FunctionalIterator<Pair<ByteArray, ByteArray>> committedIIDs() {
            return graphMgr.schema().committedIIDs();
        }

        /**
         * Commits any writes captured in the transaction into storage.
         *
         * If the transaction was opened as a {@code READ} transaction, then this
         * operation will throw an exception. If this transaction has been committed,
         * it cannot be committed again. If it has not been committed, then it will
         * flush all changes in the graph into storage by calling {@code graph.commit()},
         * which may result in acquiring a lock on the storage to confirm that the data
         * will be committed into storage. The operation will then continue to commit
         * all the writes into RocksDB by calling {@code rocksTransaction.commit()}.
         * If the operation reaches this state, then the RocksDB commit was successful.
         * We then need let go of the transaction that this resources of hold.
         *
         * If a lock was acquired from calling {@code graph.commit()} then we should
         * let inform the graph by confirming whether the RocksDB commit was successful
         * or not.
         */
        @Override
        public void commit() {
            if (isOpen.compareAndSet(true, false)) {
                try {
                    if (type().isRead()) throw TypeDBException.of(ILLEGAL_COMMIT);
                    else if (graphMgr.data().isModified()) throw TypeDBException.of(SESSION_SCHEMA_VIOLATION);

                    conceptMgr.validateTypes();
                    logicMgr.revalidateAndReindexRules();
                    graphMgr.schema().commit();
                    schemaStorage.commit();
                    session.database().cacheInvalidate();
                } catch (RocksDBException e) {
                    throw TypeDBException.of(e);
                } finally {
                    graphMgr.clear();
                    closeResources();
                    notifyClosed();
                }
            } else {
                throw TypeDBException.of(TRANSACTION_CLOSED);
            }
        }

        @Override
        public void rollback() {
            try {
                graphMgr.clear();
                schemaStorage.rollback();
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        @Override
        protected void closeResources() {
            schemaStorage.close();
            dataStorage.close();
        }
    }

    public static class Data extends CoreTransaction {

        protected final RocksStorage.Data dataStorage;
        private final CoreDatabase.Cache cache;
        final long id;

        public Data(CoreSession.Data session, Arguments.Transaction.Type type,
                    Options.Transaction options, Factory.Storage storageFactory) {
            super(session, type, options);

            id = session.database().nextTransactionID();
            cache = session.database().cacheBorrow();
            dataStorage = storageFactory.storageData(session.database(), this);
            ThingGraph thingGraph = new ThingGraph(dataStorage, cache.typeGraph());
            graphMgr = new GraphManager(cache.typeGraph(), thingGraph);

            if (type().isWrite()) session.database().consistencyMgr().opened(this);
            initialise(graphMgr, cache.traversal(), cache.logic());
        }

        @Override
        boolean isData() {
            return true;
        }

        @Override
        Data asData() {
            return this;
        }

        /**
         * Commits any writes captured in the transaction into storage.
         *
         * If the transaction was opened as a {@code READ} transaction, then this
         * operation will throw an exception. If this transaction has been committed,
         * it cannot be committed again. If it has not been committed, then it will
         * flush all changes in the graph into storage by calling {@code graph.commit()},
         * which may result in acquiring a lock on the storage to confirm that the data
         * will be committed into storage. The operation will then continue to commit
         * all the writes into RocksDB by calling {@code rocksTransaction.commit()}.
         * If the operation reaches this state, then the RocksDB commit was successful.
         * We then need let go of the transaction that this resources of hold.
         *
         * If a lock was acquired from calling {@code graph.commit()} then we should
         * let inform the graph by confirming whether the RocksDB commit was successful
         * or not.
         */
        @Override
        public void commit() {
            if (isOpen.compareAndSet(true, false)) {
                try {
                    if (type().isRead()) throw TypeDBException.of(ILLEGAL_COMMIT);
                    else if (graphMgr.schema().isModified()) throw TypeDBException.of(SESSION_DATA_VIOLATION);

                    conceptMgr.validateThings();
                    graphMgr.data().commit();

                    Set<CoreTransaction.Data> concurrentTxn;
                    synchronized (session.database().consistencyMgr()) {
                        concurrentTxn = session.database().consistencyMgr().commitMayConflict(this);
                        validateConsistency(concurrentTxn);
                        session.database().consistencyMgr().commitStarted(this);
                    }

                    recordStatisticsMetadata(concurrentTxn);
                    dataStorage.commit();
                    session.database().consistencyMgr().commitSuccessful(this);
                } catch (RocksDBException e) {
                    rollback();
                    throw TypeDBException.of(e);
                } finally {
                    graphMgr.data().clear();
                    closeResources();
                    notifyClosed();
                }
            } else {
                throw TypeDBException.of(TRANSACTION_CLOSED);
            }
        }

        private void recordStatisticsMetadata(Set<CoreTransaction.Data> concurrentTxn) {
            recordMiscounts(concurrentTxn);
            dataStorage.putUntracked(StatisticsKey.txnCommitted(id));
        }

        private void recordMiscounts(Set<Data> concurrentTxn) {
            Map<AttributeVertex.Write<?>, Set<Long>> attrOvercountDependencies = new HashMap<>();
            Map<AttributeVertex.Write<?>, Set<Long>> attrUndercountDependencies = new HashMap<>();
            Map<Pair<ThingVertex.Write, AttributeVertex.Write<?>>, Set<Long>> hasOvercountDependencies = new HashMap<>();
            Map<Pair<ThingVertex.Write, AttributeVertex.Write<?>>, Set<Long>> hasUndercountDependencies = new HashMap<>();
            for (CoreTransaction.Data concurrent : concurrentTxn) {
                graphMgr.data().stats().miscountable().attrCreatedIntersection(concurrent.graphMgr.data().stats().miscountable())
                        .forEachRemaining(attribute ->
                                attrOvercountDependencies.computeIfAbsent(attribute, (key) -> new HashSet<>()).add(concurrent.id)
                        );
                graphMgr.data().stats().miscountable().attrDeletedIntersection(concurrent.graphMgr.data().stats().miscountable())
                        .forEachRemaining(attribute ->
                                attrUndercountDependencies.computeIfAbsent(attribute, (key) -> new HashSet<>()).add(concurrent.id)
                        );
                graphMgr.data().stats().miscountable().hasCreatedIntersection(concurrent.graphMgr.data().stats().miscountable())
                        .forEachRemaining(pair ->
                                hasOvercountDependencies.computeIfAbsent(pair, (key) -> new HashSet<>()).add(concurrent.id)
                        );
                graphMgr.data().stats().miscountable().hasDeletedIntersection(concurrent.graphMgr.data().stats().miscountable())
                        .forEachRemaining(attribute ->
                                hasUndercountDependencies.computeIfAbsent(attribute, (key) -> new HashSet<>()).add(concurrent.id)
                        );
            }

            attrOvercountDependencies.forEach((attr, txs) ->
                    dataStorage.putUntracked(StatisticsKey.MisCount.attrConditionalOvercount(id, attr.iid()), encodeLongSet(txs))
            );
            attrUndercountDependencies.forEach((attr, txs) ->
                    dataStorage.putUntracked(StatisticsKey.MisCount.attrConditionalUndercount(id, attr.iid()), encodeLongSet(txs))
            );
            hasOvercountDependencies.forEach((has, txs) ->
                    dataStorage.putUntracked(StatisticsKey.MisCount.hasConditionalOvercount(id, has.first().iid(), has.second().iid()), encodeLongSet(txs))
            );
            hasUndercountDependencies.forEach((has, txs) ->
                    dataStorage.putUntracked(StatisticsKey.MisCount.hasConditionalUndercount(id, has.first().iid(), has.second().iid()), encodeLongSet(txs))
            );
        }

        private void validateConsistency(Set<CoreTransaction.Data> concurrent) {
            for (CoreTransaction.Data otherTxn : concurrent) {
                // TODO we should push the checks of intersections into the storage which checks against another storage
                validateModifiedKeys(otherTxn);
                if (hasIntersection(dataStorage.deletedKeys(), otherTxn.dataStorage.modifiedKeys())) {
                    throw TypeDBException.of(TRANSACTION_CONSISTENCY_DELETE_MODIFY_VIOLATION);
                } else if (hasIntersection(dataStorage.exclusiveBytes(), otherTxn.dataStorage.exclusiveBytes())) {
                    throw TypeDBException.of(TRANSACTION_CONSISTENCY_EXCLUSIVE_CREATE_VIOLATION);
                }
            }
        }

        private void validateModifiedKeys(CoreTransaction.Data concurrent) {
            NavigableSet<ByteArray> active = dataStorage.modifiedKeys();
            NavigableSet<ByteArray> other = concurrent.dataStorage.deletedKeys();
            if (active.isEmpty()) return;
            ByteArray currentKey = active.first();
            while (currentKey != null) {
                ByteArray otherKey = other.ceiling(currentKey);
                if (otherKey != null && otherKey.equals(currentKey)) {
                    if (dataStorage.isModifiedValidatedKey(currentKey)) {
                        throw TypeDBException.of(TRANSACTION_CONSISTENCY_MODIFY_DELETE_VIOLATION);
                    } else {
                        otherKey = other.higher(currentKey);
                    }
                }
                currentKey = otherKey;
                NavigableSet<ByteArray> tmp = other;
                other = active;
                active = tmp;
            }
        }

        @Override
        public void rollback() {
            try {
                graphMgr.data().clear();
                dataStorage.rollback();
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        }

        @Override
        protected void closeResources() {
            session.database().cacheUnborrow(cache);
            dataStorage.close();
        }

        @Override
        void notifyClosed() {
            if (type().isWrite()) {
                session.database().consistencyMgr().closed(this);
                session.database().statisticsSynchroniser().transactionClosed(id);
            }
            super.notifyClosed();
        }

        @Override
        public FunctionalIterator<Pair<ByteArray, ByteArray>> committedIIDs() {
            return graphMgr.data().committedIIDs();
        }

        public Long snapshotStart() {
            return dataStorage.snapshotStart();
        }

        public Optional<Long> snapshotEnd() {
            return dataStorage.snapshotEnd();
        }
    }
}
