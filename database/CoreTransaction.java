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
import com.vaticle.typedb.core.logic.LogicCache;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.query.QueryManager;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typedb.core.traversal.TraversalCache;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import org.rocksdb.RocksDBException;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_COMMIT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ISOLATION_DELETE_MODIFY_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ISOLATION_EXCLUSIVE_CREATE_VIOLATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ISOLATION_MODIFY_DELETE_VIOLATION;

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
            closeResources();
            notifyClosed();
            delete();
        }
    }

    protected void notifyClosed() {
        session.closed(this);
    }

    abstract void delete();

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

        protected TypeGraph graph() {
            return graphMgr.schema();
        }

        protected RocksStorage.Schema schemaStorage() {
            return schemaStorage;
        }

        protected RocksStorage.Data dataStorage() {
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
                    closeResources();
                    notifyClosed();
                    delete();
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

        @Override
        public void delete() {
            assert !isOpen.get();
            graphMgr.clear();
        }
    }

    public static class Data extends CoreTransaction {

        protected final RocksStorage.Data dataStorage;
        private final CoreDatabase.Cache cache;
        final long id;

        public Data(CoreSession.Data session, Arguments.Transaction.Type type,
                    Options.Transaction options, Factory.Storage storageFactory) {
            super(session, type, options);

            this.id = session.database().nextTransactionID();
            this.cache = session.database().cacheBorrow();
            this.dataStorage = storageFactory.storageData(session.database(), this);
            ThingGraph thingGraph = new ThingGraph(dataStorage, cache.typeGraph());
            this.graphMgr = new GraphManager(cache.typeGraph(), thingGraph);

            if (type().isWrite()) session.database().isolationMgr().opened(this);
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

                    Set<CoreTransaction.Data> overlapping = session.database().isolationMgr().validateOverlappingAndStartCommit(this);
                    session.database().statisticsCorrector().recordCorrectionMetadata(this, overlapping);
                    dataStorage.commit();
                    session.database().isolationMgr().committed(this);
                    session.database().statisticsCorrector().committed(this);
                } catch (TypeDBException e) {
                    delete();
                    throw e;
                } catch (RocksDBException e) {
                    delete();
                    throw TypeDBException.of(e);
                } finally {
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
        protected void notifyClosed() {
            if (type().isWrite()) session.database().isolationMgr().closed(this);
            super.notifyClosed();
        }

        @Override
        public void delete() {
            assert !isOpen.get();
            graphMgr.data().clear();
            dataStorage.delete();
            session.database().statisticsCorrector().deleted(this);
        }

        @Override
        public FunctionalIterator<Pair<ByteArray, ByteArray>> committedIIDs() {
            return graphMgr.data().committedIIDs();
        }

        public long snapshotStart() {
            return dataStorage.snapshotStart();
        }

        public Optional<Long> snapshotEnd() {
            return dataStorage.snapshotEnd();
        }
    }
}
