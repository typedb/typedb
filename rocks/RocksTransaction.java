/*
 * Copyright (C) 2020 Grakn Labs
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
import grakn.core.logic.tool.TypeHinter;
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

    final RocksSession session;
    final Arguments.Transaction.Type type;
    final Context.Transaction context;
    GraphManager graphMgr;
    ConceptManager conceptMgr;
    LogicManager logicMgr;
    Reasoner reasoner;
    QueryManager queryMgr;
    AtomicBoolean isOpen;

    private RocksTransaction(RocksSession session, Arguments.Transaction.Type type, Options.Transaction options) {
        this.type = type;
        this.session = session;
        context = new Context.Transaction(session.context(), options).type(type);
    }

    void initialise(GraphManager graphMgr, TraversalCache traversalCache, LogicCache logicCache) {
        TraversalEngine traversalEngine = new TraversalEngine(graphMgr, traversalCache);
        conceptMgr = new ConceptManager(graphMgr);
        logicMgr = new LogicManager(graphMgr, conceptMgr, traversalEngine, logicCache);
        reasoner = new Reasoner(conceptMgr, traversalEngine, logicMgr);
        queryMgr = new QueryManager(conceptMgr, logicMgr, reasoner, context);
        isOpen = new AtomicBoolean(true);
    }

    public Context.Transaction context() {
        return context;
    }

    @Override
    public Arguments.Transaction.Type type() {
        return type;
    }

    @Override
    public Options.Transaction options() {
        return context.options();
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public QueryManager query() {
        if (!isOpen.get()) throw new GraknException(TRANSACTION_CLOSED);
        return queryMgr;
    }

    @Override
    public ConceptManager concepts() {
        if (!isOpen.get()) throw new GraknException(TRANSACTION_CLOSED);
        return conceptMgr;
    }

    @Override
    public LogicManager logics() {
        if (!isOpen.get()) throw new GraknException(TRANSACTION_CLOSED);
        return logicMgr;
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            closeResources();
        }
    }

    void closeResources() {
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
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Schema.class)));
    }

    Data asData() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Data.class)));
    }

    public static class Schema extends RocksTransaction {

        private final RocksStorage.Schema schemaStorage;
        private final RocksStorage.Data dataStorage;

        Schema(RocksSession.Schema session, Arguments.Transaction.Type type, Options.Transaction options) {
            super(session, type, options);

            schemaStorage = new RocksStorage.Schema(session.database, this);
            SchemaGraph schemaGraph = new SchemaGraph(schemaStorage, type.isRead());

            dataStorage = new RocksStorage.Data(session.database, this);
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
            if (!isOpen.get()) throw new GraknException(TRANSACTION_CLOSED);
            return schemaStorage;
        }

        RocksStorage.Data dataStorage() {
            if (!isOpen.get()) throw new GraknException(TRANSACTION_CLOSED);
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
                    if (type.isRead()) throw new GraknException(ILLEGAL_COMMIT);
                    else if (graphMgr.data().isModified()) throw new GraknException(SESSION_SCHEMA_VIOLATION);

                    // We disable RocksDB indexing of uncommitted writes, as we're only about to write and never again reading
                    // TODO: We should benchmark this
                    schemaStorage.rocksTx.disableIndexing();
                    conceptMgr.validateTypes();
                    logicMgr.validateRules();
                    graphMgr.schema().commit();
                    schemaStorage.rocksTx.commit();
                    session.database.invalidateCache();
                } catch (RocksDBException e) {
                    rollback();
                    throw new GraknException(e);
                } finally {
                    graphMgr.clear();
                    closeResources();
                }
            } else {
                throw new GraknException(TRANSACTION_CLOSED);
            }
        }

        @Override
        public void rollback() {
            try {
                graphMgr.clear();
                schemaStorage.rocksTx.rollback();
            } catch (RocksDBException e) {
                throw new GraknException(e);
            }
        }

        @Override
        void closeStorage() {
            schemaStorage.close();
            dataStorage.close();
        }
    }

    public static class Data extends RocksTransaction {
        private final RocksStorage.Data dataStorage;
        private final RocksDatabase.Cache cache;

        public Data(RocksSession.Data session, Arguments.Transaction.Type type, Options.Transaction options) {
            super(session, type, options);

            cache = session.database.borrowCache();
            dataStorage = new RocksStorage.Data(session.database, this);
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
                    if (type.isRead()) throw new GraknException(ILLEGAL_COMMIT);
                    else if (graphMgr.schema().isModified()) throw new GraknException(SESSION_DATA_VIOLATION);

                    // We disable RocksDB indexing of uncommitted writes, as we're only about to write and never again reading
                    // TODO: We should benchmark this
                    dataStorage.rocksTx.disableIndexing();
                    conceptMgr.validateThings();
                    graphMgr.data().commit();
                    dataStorage.rocksTx.commit();
                    if (graphMgr.data().stats().needsBackgroundCounting()) {
                        session.database.statisticsBackgroundCounter.needsBackgroundCounting();
                    }
                } catch (RocksDBException e) {
                    rollback();
                    throw new GraknException(e);
                } finally {
                    graphMgr.data().clear();
                    closeResources();
                }
            } else {
                throw new GraknException(TRANSACTION_CLOSED);
            }
        }

        @Override
        public void rollback() {
            try {
                graphMgr.clear();
                dataStorage.rocksTx.rollback();
            } catch (RocksDBException e) {
                throw new GraknException(e);
            }
        }

        @Override
        void closeStorage() {
            session.database.unborrowCache(cache);
            dataStorage.close();
        }
    }
}
