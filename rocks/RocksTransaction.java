/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.rocks;

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.graph.DataGraph;
import grakn.core.graph.GraphManager;
import grakn.core.graph.SchemaGraph;
import grakn.core.logic.LogicCache;
import grakn.core.logic.LogicManager;
import grakn.core.query.QueryManager;
import grakn.core.reasoner.Reasoner;
import grakn.core.traversal.TraversalCache;
import grakn.core.traversal.TraversalEngine;
import org.rocksdb.RocksDBException;

import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Transaction.ILLEGAL_COMMIT;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_DATA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.SESSION_SCHEMA_VIOLATION;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CLOSED;

public abstract class RocksTransaction implements Grakn.Transaction {

    protected final RocksSession session;
    protected final Context.Transaction context;
    protected GraphManager graphMgr;
    protected ConceptManager conceptMgr;
    protected AtomicBoolean isOpen;
    TraversalEngine traversalEng;
    LogicManager logicMgr;
    Reasoner reasoner;
    QueryManager queryMgr;

    private RocksTransaction(RocksSession session, Arguments.Transaction.Type type, Options.Transaction options) {
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
        if (!isOpen.get()) throw GraknException.of(TRANSACTION_CLOSED);
        return queryMgr;
    }

    @Override
    public ConceptManager concepts() {
        if (!isOpen.get()) throw GraknException.of(TRANSACTION_CLOSED);
        return conceptMgr;
    }

    @Override
    public LogicManager logic() {
        if (!isOpen.get()) throw GraknException.of(TRANSACTION_CLOSED);
        return logicMgr;
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            closeResources();
        }
    }

    public Reasoner reasoner() {
        return reasoner;
    }

    protected void closeResources() {
        closeStorage();
        session.remove(this);
    }

    abstract void closeStorage();

    boolean isSchema() {
        return false;
    }

    boolean isData() {
        return false;
    }

    Schema asSchema() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Schema.class));
    }

    Data asData() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Data.class));
    }

    public static class Schema extends RocksTransaction {

        protected final RocksStorage.Schema schemaStorage;
        protected final RocksStorage.Data dataStorage;

        protected Schema(RocksSession.Schema session, Arguments.Transaction.Type type,
                         Options.Transaction options, Factory.Storage storageFactory) {
            super(session, type, options);

            schemaStorage = storageFactory.storageSchema(session.database(), this);
            SchemaGraph schemaGraph = new SchemaGraph(schemaStorage, type().isRead());

            dataStorage = storageFactory.storageData(session.database(), this);
            DataGraph dataGraph = new DataGraph(dataStorage, schemaGraph);

            graphMgr = new GraphManager(schemaGraph, dataGraph);
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

        SchemaGraph graph() {
            return graphMgr.schema();
        }

        RocksStorage.Schema schemaStorage() {
            if (!isOpen.get()) throw GraknException.of(TRANSACTION_CLOSED);
            return schemaStorage;
        }

        RocksStorage.Data dataStorage() {
            if (!isOpen.get()) throw GraknException.of(TRANSACTION_CLOSED);
            return dataStorage;
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
                    if (type().isRead()) throw GraknException.of(ILLEGAL_COMMIT);
                    else if (graphMgr.data().isModified()) throw GraknException.of(SESSION_SCHEMA_VIOLATION);

                    conceptMgr.validateTypes();
                    logicMgr.revalidateAndReindexRules();
                    graphMgr.schema().commit();
                    schemaStorage.commit();
                    session.database().cacheInvalidate();
                } catch (RocksDBException e) {
                    rollback();
                    throw GraknException.of(e);
                } finally {
                    graphMgr.clear();
                    closeResources();
                }
            } else {
                throw GraknException.of(TRANSACTION_CLOSED);
            }
        }

        @Override
        public void rollback() {
            try {
                graphMgr.clear();
                schemaStorage.rollback();
            } catch (RocksDBException e) {
                throw GraknException.of(e);
            }
        }

        @Override
        void closeStorage() {
            schemaStorage.close();
            dataStorage.close();
        }
    }

    public static class Data extends RocksTransaction {

        protected final RocksStorage.Data dataStorage;
        private final RocksDatabase.Cache cache;

        public Data(RocksSession.Data session, Arguments.Transaction.Type type,
                    Options.Transaction options, Factory.Storage storageFactory) {
            super(session, type, options);

            cache = session.database().cacheBorrow();
            dataStorage = storageFactory.storageData(session.database(), this);
            DataGraph dataGraph = new DataGraph(dataStorage, cache.schemaGraph());
            graphMgr = new GraphManager(cache.schemaGraph(), dataGraph);

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
                    if (type().isRead()) throw GraknException.of(ILLEGAL_COMMIT);
                    else if (graphMgr.schema().isModified()) throw GraknException.of(SESSION_DATA_VIOLATION);

                    conceptMgr.validateThings();
                    graphMgr.data().commit();
                    dataStorage.commit();
                    triggerStatisticBgCounter();
                } catch (RocksDBException e) {
                    rollback();
                    throw GraknException.of(e);
                } finally {
                    graphMgr.data().clear();
                    closeResources();
                }
            } else {
                throw GraknException.of(TRANSACTION_CLOSED);
            }
        }

        @Override
        public void rollback() {
            try {
                graphMgr.clear();
                dataStorage.rollback();
            } catch (RocksDBException e) {
                throw GraknException.of(e);
            }
        }

        @Override
        void closeStorage() {
            session.database().cacheUnborrow(cache);
            dataStorage.close();
        }

        /**
         * Responsible for triggering {@link RocksDatabase.StatisticsBackgroundCounter}, if necessary.
         * A different implementation of this class may override it.
         */
        protected void triggerStatisticBgCounter() {
            if (graphMgr.data().stats().needsBackgroundCounting()) {
                session.database().statisticsBackgroundCounter.needsBackgroundCounting();
            }
        }
    }
}
