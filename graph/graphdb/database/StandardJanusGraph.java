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
 */

package grakn.core.graph.graphdb.database;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.diskstorage.Backend;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BackendTransaction;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.indexing.IndexEntry;
import grakn.core.graph.diskstorage.indexing.IndexTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyIterator;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRangeQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSCache;
import grakn.core.graph.diskstorage.log.Log;
import grakn.core.graph.diskstorage.log.Message;
import grakn.core.graph.diskstorage.log.kcvs.KCVSLog;
import grakn.core.graph.diskstorage.util.RecordIterator;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.database.cache.SchemaCache;
import grakn.core.graph.graphdb.database.cache.StandardSchemaCache;
import grakn.core.graph.graphdb.database.idassigner.VertexIDAssigner;
import grakn.core.graph.graphdb.database.idhandling.IDHandler;
import grakn.core.graph.graphdb.database.log.LogTxStatus;
import grakn.core.graph.graphdb.database.log.TransactionLogHeader;
import grakn.core.graph.graphdb.database.management.ManagementSystem;
import grakn.core.graph.graphdb.database.serialize.StandardSerializer;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.InternalVertexLabel;
import grakn.core.graph.graphdb.query.QueryUtil;
import grakn.core.graph.graphdb.relations.EdgeDirection;
import grakn.core.graph.graphdb.tinkerpop.JanusGraphFeatures;
import grakn.core.graph.graphdb.tinkerpop.optimize.AdjacentVertexFilterOptimizerStrategy;
import grakn.core.graph.graphdb.tinkerpop.optimize.JanusGraphLocalQueryOptimizerStrategy;
import grakn.core.graph.graphdb.tinkerpop.optimize.JanusGraphStepStrategy;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.transaction.StandardTransactionBuilder;
import grakn.core.graph.graphdb.transaction.TransactionConfiguration;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.MixedIndexType;
import grakn.core.graph.graphdb.types.system.BaseKey;
import grakn.core.graph.graphdb.types.system.BaseRelationType;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StandardJanusGraph implements JanusGraph {

    private static final Logger LOG = LoggerFactory.getLogger(StandardJanusGraph.class);

    // Filters used to retain Schema vertices(SCHEMA_FILTER), NOT Schema vertices(NO_SCHEMA_FILTER) and to not filter anything (NO_FILTER).
    private static final Predicate<InternalRelation> SCHEMA_FILTER = internalRelation -> internalRelation.getType() instanceof BaseRelationType && internalRelation.getVertex(0) instanceof JanusGraphSchemaVertex;
    private static final Predicate<InternalRelation> NO_SCHEMA_FILTER = internalRelation -> !SCHEMA_FILTER.test(internalRelation);
    private static final Predicate<InternalRelation> NO_FILTER = internalRelation -> true;

    static {
        TraversalStrategies graphStrategies = TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                .addStrategies(
                        AdjacentVertexFilterOptimizerStrategy.instance(),
                        JanusGraphLocalQueryOptimizerStrategy.instance(),
                        JanusGraphStepStrategy.instance()
                );

        //Register with cache
        TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraphTx.class, graphStrategies);
    }

    private final GraphDatabaseConfiguration config;
    private final Backend backend;
    private final IDManager idManager;
    private final VertexIDAssigner idAssigner;
    private final TimestampProvider timestampProvider;

    //Serializers
    private final IndexSerializer indexSerializer;
    private final EdgeSerializer edgeSerializer;
    protected final StandardSerializer serializer;

    //Caches
    public final SliceQuery vertexExistenceQuery;
    private final RelationQueryCache queryCache;
    private final SchemaCache schemaCache;

    private volatile boolean isOpen;
    private final AtomicLong txCounter; // used to generate unique transaction IDs

    private final Set<StandardJanusGraphTx> openTransactions;

    public StandardJanusGraph(GraphDatabaseConfiguration configuration, Backend backend) {
        this.config = configuration;
        this.isOpen = true;
        this.txCounter = new AtomicLong(0);
        this.openTransactions = Collections.newSetFromMap(new ConcurrentHashMap<>(100, 0.75f, 1));

        // Collaborators:
        this.backend = backend;
        this.idAssigner = new VertexIDAssigner(config.getConfiguration(), backend);
        this.idManager = idAssigner.getIDManager();
        this.timestampProvider = configuration.getTimestampProvider();


        // Collaborators (Serializers)
        this.serializer = new StandardSerializer();
        StoreFeatures storeFeatures = backend.getStoreFeatures();
        this.indexSerializer = new IndexSerializer(configuration.getConfiguration(), this.serializer, this.backend.getIndexInformation(), storeFeatures.isDistributed() && storeFeatures.isKeyOrdered());
        this.edgeSerializer = new EdgeSerializer(this.serializer);

        // The following query is used by VertexConstructors(inside JanusTransaction) to check whether a vertex associated to a specific ID actually exists in the DB (and it's not a ghost)
        // Full explanation on why this query is used: https://github.com/thinkaurelius/titan/issues/214
        this.vertexExistenceQuery = edgeSerializer.getQuery(BaseKey.VertexExists, Direction.OUT, new EdgeSerializer.TypedInterval[0]).setLimit(1);

        // Collaborators (Caches)
        this.queryCache = new RelationQueryCache(this.edgeSerializer);
        this.schemaCache = new StandardSchemaCache(typeCacheRetrieval);
    }

    @Override
    public String toString() {
        return "StandardJanusGraph[" + backend.getStoreManager().getName() + "]";
    }

    public org.apache.commons.configuration.Configuration configuration() {
        return getConfiguration().getConfigurationAtOpen();
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

    @Override
    public synchronized void close() throws JanusGraphException {
        if (!isOpen) return;

        Map<JanusGraphTransaction, RuntimeException> txCloseExceptions = new HashMap<>();

        try {
            /* Assuming a couple of properties about openTransactions:
             * 1. no concurrent modifications during graph shutdown
             * 2. all contained localJanusTransaction are open
             */
            for (StandardJanusGraphTx otx : openTransactions) {
                try {
                    otx.rollback();
                    otx.close();
                } catch (RuntimeException e) {
                    // Catch and store these exceptions, but proceed with the loop
                    // Any remaining localJanusTransaction on the iterator should get a chance to close before we throw up
                    LOG.warn("Unable to close transaction {}", otx, e);
                    txCloseExceptions.put(otx, e);
                }
            }
            idAssigner.close();
            backend.close();
        } finally {
            isOpen = false;
        }

        // Throw an exception if at least one transaction failed to close
        if (1 == txCloseExceptions.size()) {
            // TP3's test suite requires that this be of type ISE
            throw new IllegalStateException("Unable to close transaction", Iterables.getOnlyElement(txCloseExceptions.values()));
        } else if (1 < txCloseExceptions.size()) {
            throw new IllegalStateException(String.format("Unable to close %s transactions (see warnings in LOG output for details)",
                    txCloseExceptions.size()));
        }
    }

    // ################### Simple Getters #########################

    public Graph.Features features() {
        return JanusGraphFeatures.getFeatures(this, backend.getStoreFeatures());
    }

    public IndexSerializer getIndexSerializer() {
        return indexSerializer;
    }

    public IDManager getIDManager() {
        return idManager;
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public StandardSerializer getDataSerializer() {
        return serializer;
    }

    public SchemaCache getSchemaCache() {
        return schemaCache;
    }

    public GraphDatabaseConfiguration getConfiguration() {
        return config;
    }

    @Override
    public JanusGraphManagement openManagement() {
        return new ManagementSystem(this, backend.getGlobalSystemConfig());
    }

    public Set<? extends JanusGraphTransaction> getOpenTransactions() {
        return Sets.newHashSet(openTransactions);
    }

    // ################### TRANSACTIONS #########################

    @Override
    public JanusGraphTransaction newTransaction() {
        return buildTransaction().start();
    }

    @Override
    public StandardTransactionBuilder buildTransaction() {
        return new StandardTransactionBuilder(getConfiguration(), this);
    }

    public StandardJanusGraphTx newThreadBoundTransaction() {
        return buildTransaction().threadBound().start();
    }

    public StandardJanusGraphTx newTransaction(TransactionConfiguration configuration) {
        if (!isOpen) {
            throw new IllegalStateException("Graph has been shut down");
        }
        StandardJanusGraphTx tx = new StandardJanusGraphTx(this, configuration);
        openTransactions.add(tx);
        return tx;
    }

    // This in only used from StandardJanusGraphTx (that's why public when it's really a private method) for an awkward initialisation that should be fixed in the future
    public BackendTransaction openBackendTransaction(StandardJanusGraphTx tx) {
        try {
            IndexSerializer.IndexInfoRetriever retriever = indexSerializer.getIndexInfoRetriever(tx);
            return backend.beginTransaction(tx.getConfiguration(), retriever);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not start new transaction", e);
        }
    }

    public void closeTransaction(StandardJanusGraphTx tx) {
        openTransactions.remove(tx);
    }

    // ################### READ #########################

    private final SchemaCache.StoreRetrieval typeCacheRetrieval = new SchemaCache.StoreRetrieval() {

        @Override
        public Long retrieveSchemaByName(String typeName) {
            // Get a consistent tx
            Configuration customTxOptions = backend.getStoreFeatures().getKeyConsistentTxConfig();
            StandardJanusGraphTx consistentTx = null;
            try {
                consistentTx = StandardJanusGraph.this.newTransaction(
                        new StandardTransactionBuilder(getConfiguration(), StandardJanusGraph.this, customTxOptions)
                );
                consistentTx.getBackendTransaction().disableCache();
                JanusGraphVertex v = Iterables.getOnlyElement(QueryUtil.getVertices(consistentTx, BaseKey.SchemaName, typeName), null);
                return v != null ? v.longId() : null;
            } finally {
                try {
                    if (consistentTx != null) {
                        consistentTx.rollback();
                    }
                } catch (Throwable t) {
                    LOG.warn("Unable to rollback transaction", t);
                }
            }
        }

        @Override
        public EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
            SliceQuery query = queryCache.getQuery(type, dir);
            Configuration customTxOptions = backend.getStoreFeatures().getKeyConsistentTxConfig();
            StandardJanusGraphTx consistentTx = null;
            try {
                consistentTx = StandardJanusGraph.this.newTransaction(new StandardTransactionBuilder(getConfiguration(), StandardJanusGraph.this, customTxOptions));
                consistentTx.getBackendTransaction().disableCache();
                return edgeQuery(schemaId, query, consistentTx.getBackendTransaction());
            } finally {
                try {
                    if (consistentTx != null) {
                        consistentTx.rollback();
                    }
                } catch (Throwable t) {
                    LOG.warn("Unable to rollback transaction", t);
                }
            }
        }

    };

    public RecordIterator<Long> getVertexIDs(BackendTransaction tx) {
        Preconditions.checkArgument(backend.getStoreFeatures().hasOrderedScan() || backend.getStoreFeatures().hasUnorderedScan(),
                "The configured storage backend does not support global graph operations - use Faunus instead");

        KeyIterator keyIterator;
        if (backend.getStoreFeatures().hasUnorderedScan()) {
            keyIterator = tx.edgeStoreKeys(vertexExistenceQuery);
        } else {
            keyIterator = tx.edgeStoreKeys(new KeyRangeQuery(IDHandler.MIN_KEY, IDHandler.MAX_KEY, vertexExistenceQuery));
        }

        return new RecordIterator<Long>() {

            @Override
            public boolean hasNext() {
                return keyIterator.hasNext();
            }

            @Override
            public Long next() {
                return idManager.getKeyID(keyIterator.next());
            }

            @Override
            public void close() throws IOException {
                keyIterator.close();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Removal not supported");
            }
        };
    }

    public EntryList edgeQuery(long vid, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vid > 0);
        return tx.edgeStoreQuery(new KeySliceQuery(idManager.getKey(vid), query));
    }

    public List<EntryList> edgeMultiQuery(List<Long> vertexIdsAsLongs, SliceQuery query, BackendTransaction tx) {
        List<StaticBuffer> vertexIds = vertexIdsAsLongs.stream().map(idManager::getKey).collect(Collectors.toList());
        Map<StaticBuffer, EntryList> result = tx.edgeStoreMultiQuery(vertexIds, query);
        List<EntryList> resultList = new ArrayList<>(result.size());
        for (StaticBuffer v : vertexIds) resultList.add(result.get(v));
        return resultList;
    }

    // ################### WRITE #########################

    public void assignID(InternalRelation relation) {
        idAssigner.assignID(relation);
    }

    public void assignID(InternalVertex vertex, VertexLabel label) {
        idAssigner.assignID(vertex, label);
    }

    /**
     * The TTL of a relation (edge or property) is the minimum of:
     * 1) The TTL configured of the relation type (if exists)
     * 2) The TTL configured for the label any of the relation end point vertices (if exists)
     *
     * @param rel relation to determine the TTL for
     */
    public static int getTTL(InternalRelation rel) {
        InternalRelationType baseType = (InternalRelationType) rel.getType();
        int ttl = 0;
        Integer ettl = baseType.getTTL();
        if (ettl > 0) ttl = ettl;
        for (int i = 0; i < rel.getArity(); i++) {
            int vttl = getTTL(rel.getVertex(i));
            if (vttl > 0 && (vttl < ttl || ttl <= 0)) ttl = vttl;
        }
        return ttl;
    }

    public static int getTTL(InternalVertex v) {
        if (IDManager.VertexIDType.UnmodifiableVertex.is(v.longId())) {
            return ((InternalVertexLabel) v.vertexLabel()).getTTL();
        } else return 0;
    }

    private static class ModificationSummary {
        final boolean hasModifications;
        final boolean has2iModifications;

        private ModificationSummary(boolean hasModifications, boolean has2iModifications) {
            this.hasModifications = hasModifications;
            this.has2iModifications = has2iModifications;
        }
    }

    private ModificationSummary prepareCommit(Collection<InternalRelation> addedRelations, Collection<InternalRelation> deletedRelations,
                                              Predicate<InternalRelation> filter, BackendTransaction mutator, StandardJanusGraphTx tx) throws BackendException {
        ListMultimap<Long, InternalRelation> mutations = ArrayListMultimap.create();
        ListMultimap<InternalVertex, InternalRelation> mutatedProperties = ArrayListMultimap.create();
        List<IndexSerializer.IndexUpdate> indexUpdates = Lists.newArrayList();
        //1) Collect deleted edges and their index updates and acquire edge locks
        for (InternalRelation del : Iterables.filter(deletedRelations, filter::test)) {
            Preconditions.checkArgument(del.isRemoved());
            for (int pos = 0; pos < del.getLen(); pos++) {
                InternalVertex vertex = del.getVertex(pos);
                if (pos == 0 || !del.isLoop()) {
                    if (del.isProperty()) mutatedProperties.put(vertex, del);
                    mutations.put(vertex.longId(), del);
                }
            }
            indexUpdates.addAll(indexSerializer.getIndexUpdates(del));
        }

        //2) Collect added edges and their index updates and acquire edge locks
        for (InternalRelation add : Iterables.filter(addedRelations, filter::test)) {
            Preconditions.checkArgument(add.isNew());

            for (int pos = 0; pos < add.getLen(); pos++) {
                InternalVertex vertex = add.getVertex(pos);
                if (pos == 0 || !add.isLoop()) {
                    if (add.isProperty()) mutatedProperties.put(vertex, add);
                    mutations.put(vertex.longId(), add);
                }
            }
            indexUpdates.addAll(indexSerializer.getIndexUpdates(add));
        }

        //3) Collect all index update for vertices
        for (InternalVertex v : mutatedProperties.keySet()) {
            indexUpdates.addAll(indexSerializer.getIndexUpdates(v, mutatedProperties.get(v)));
        }
        //4) Acquire index locks (deletions first)
        for (IndexSerializer.IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || !update.isDeletion()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
        }
        for (IndexSerializer.IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || !update.isAddition()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
        }

        //5) Add relation mutations
        for (Long vertexId : mutations.keySet()) {
            Preconditions.checkArgument(vertexId > 0, "Vertex has no id: %s", vertexId);
            List<InternalRelation> edges = mutations.get(vertexId);
            List<Entry> additions = new ArrayList<>(edges.size());
            List<Entry> deletions = new ArrayList<>(Math.max(10, edges.size() / 10));
            for (InternalRelation edge : edges) {
                InternalRelationType baseType = (InternalRelationType) edge.getType();

                for (InternalRelationType type : baseType.getRelationIndexes()) {
                    if (type.getStatus() == SchemaStatus.DISABLED) continue;
                    for (int pos = 0; pos < edge.getArity(); pos++) {
                        if (!type.isUnidirected(Direction.BOTH) && !type.isUnidirected(EdgeDirection.fromPosition(pos))) {
                            continue; //Directionality is not covered
                        }
                        if (edge.getVertex(pos).longId() == vertexId) {
                            StaticArrayEntry entry = edgeSerializer.writeRelation(edge, type, pos, tx);
                            if (edge.isRemoved()) {
                                deletions.add(entry);
                            } else {
                                Preconditions.checkArgument(edge.isNew());
                                int ttl = getTTL(edge);
                                if (ttl > 0) {
                                    entry.setMetaData(EntryMetaData.TTL, ttl);
                                }
                                additions.add(entry);
                            }
                        }
                    }
                }
            }

            StaticBuffer vertexKey = idManager.getKey(vertexId);
            mutator.mutateEdges(vertexKey, additions, deletions);
        }

        //6) Add index updates
        boolean has2iMods = false;
        for (IndexSerializer.IndexUpdate indexUpdate : indexUpdates) {
            if (indexUpdate.isCompositeIndex()) {
                IndexSerializer.IndexUpdate<StaticBuffer, Entry> update = indexUpdate;
                if (update.isAddition()) {
                    mutator.mutateIndex(update.getKey(), Lists.newArrayList(update.getEntry()), KCVSCache.NO_DELETIONS);
                } else {
                    mutator.mutateIndex(update.getKey(), KeyColumnValueStore.NO_ADDITIONS, Lists.newArrayList(update.getEntry()));
                }
            } else {
                IndexSerializer.IndexUpdate<String, IndexEntry> update = indexUpdate;
                has2iMods = true;
                IndexTransaction itx = mutator.getIndexTransaction(update.getIndex().getBackingIndexName());
                String indexStore = ((MixedIndexType) update.getIndex()).getStoreName();
                if (update.isAddition()) {
                    itx.add(indexStore, update.getKey(), update.getEntry(), update.getElement().isNew());
                } else {
                    itx.delete(indexStore, update.getKey(), update.getEntry().field, update.getEntry().value, update.getElement().isRemoved());
                }
            }
        }
        return new ModificationSummary(!mutations.isEmpty(), has2iMods);
    }

    public void commit(Collection<InternalRelation> addedRelations, Collection<InternalRelation> deletedRelations, StandardJanusGraphTx tx) {
        if (addedRelations.isEmpty() && deletedRelations.isEmpty()) {
            return;
        }
        //1. Finalise transaction
        LOG.debug("Saving transaction. Added {}, removed {}", addedRelations.size(), deletedRelations.size());
        if (!tx.getConfiguration().hasCommitTime()) tx.getConfiguration().setCommitTime(timestampProvider.getTime());
        Instant txTimestamp = tx.getConfiguration().getCommitTime();
        long transactionId = txCounter.incrementAndGet();

        //2. Assign JanusGraphVertex IDs
        if (!tx.getConfiguration().hasAssignIDsImmediately()) {
            idAssigner.assignIDs(addedRelations);
        }

        //3. Commit
        BackendTransaction mutator = tx.getBackendTransaction();
        boolean hasTxIsolation = backend.getStoreFeatures().hasTxIsolation();
        boolean logTransaction = config.hasLogTransactions() && !tx.getConfiguration().hasEnabledBatchLoading();
        KCVSLog txLog = logTransaction ? backend.getSystemTxLog() : null;
        TransactionLogHeader txLogHeader = new TransactionLogHeader(transactionId, txTimestamp, timestampProvider);
        ModificationSummary commitSummary;

        try {
            //3.1 Log transaction (write-ahead LOG) if enabled
            if (logTransaction) {
                //[FAILURE] Inability to LOG transaction fails the transaction by escalation since it's likely due to unavailability of primary
                //storage backend.
                Preconditions.checkNotNull(txLog, "Transaction LOG is null");
                txLog.add(txLogHeader.serializeModifications(serializer, LogTxStatus.PRECOMMIT, tx, addedRelations, deletedRelations), txLogHeader.getLogKey());
            }

            //3.2 Commit schema elements and their associated relations in a separate transaction if backend does not support
            //    transactional isolation
            boolean hasSchemaElements = !Iterables.isEmpty(Iterables.filter(deletedRelations, SCHEMA_FILTER::test)) || !Iterables.isEmpty(Iterables.filter(addedRelations, SCHEMA_FILTER::test));

            if (hasSchemaElements && !hasTxIsolation) {
                /*
                 * On storage without transactional isolation, create separate
                 * backend transaction for schema aspects to make sure that
                 * those are persisted prior to and independently of other
                 * mutations in the tx. If the storage supports transactional
                 * isolation, then don't create a separate tx.
                 */
                BackendTransaction schemaMutator = openBackendTransaction(tx);

                try {
                    //[FAILURE] If the preparation throws an exception abort directly - nothing persisted since batch-loading cannot be enabled for schema elements
                    prepareCommit(addedRelations, deletedRelations, SCHEMA_FILTER, schemaMutator, tx);
                } catch (Throwable e) {
                    //Roll back schema tx and escalate exception
                    schemaMutator.rollback();
                    throw e;
                }

                try {
                    schemaMutator.commit();
                } catch (Throwable e) {
                    //[FAILURE] Primary persistence failed => abort and escalate exception, nothing should have been persisted
                    LOG.error("Could not commit transaction [" + transactionId + "] due to storage exception in system-commit", e);
                    throw e;
                }
            }

            //[FAILURE] Exceptions during preparation here cause the entire transaction to fail on transactional systems
            //or just the non-system part on others. Nothing has been persisted unless batch-loading
            commitSummary = prepareCommit(addedRelations, deletedRelations, hasTxIsolation ? NO_FILTER : NO_SCHEMA_FILTER, mutator, tx);
            if (commitSummary.hasModifications) {
                String logTxIdentifier = tx.getConfiguration().getLogIdentifier();
                boolean hasSecondaryPersistence = logTxIdentifier != null || commitSummary.has2iModifications;

                //1. Commit storage - failures lead to immediate abort

                //1a. Add success message to tx LOG which will be committed atomically with all transactional changes so that we can recover secondary failures
                //    This should not throw an exception since the mutations are just cached. If it does, it will be escalated since its critical
                if (logTransaction) {
                    txLog.add(txLogHeader.serializePrimary(serializer,
                            hasSecondaryPersistence ? LogTxStatus.PRIMARY_SUCCESS : LogTxStatus.COMPLETE_SUCCESS),
                            txLogHeader.getLogKey(), mutator.getTxLogPersistor());
                }

                try {
                    mutator.commitStorage();
                } catch (Throwable e) {
                    //[FAILURE] If primary storage persistence fails abort directly (only schema could have been persisted)
                    LOG.error("Could not commit transaction [" + transactionId + "] due to storage exception in commit", e);
                    throw e;
                }

                if (hasSecondaryPersistence) {
                    LogTxStatus status = LogTxStatus.SECONDARY_SUCCESS;
                    Map<String, Throwable> indexFailures = ImmutableMap.of();
                    boolean userlogSuccess = true;

                    try {
                        //2. Commit indexes - [FAILURE] all exceptions are collected and logged but nothing is aborted
                        indexFailures = mutator.commitIndexes();
                        if (!indexFailures.isEmpty()) {
                            status = LogTxStatus.SECONDARY_FAILURE;
                            for (Map.Entry<String, Throwable> entry : indexFailures.entrySet()) {
                                LOG.error("Error while committing index mutations for transaction [" + transactionId + "] on index: " + entry.getKey(), entry.getValue());
                            }
                        }
                        //3. Log transaction if configured - [FAILURE] is recorded but does not cause exception
                        if (logTxIdentifier != null) {
                            try {
                                userlogSuccess = false;
                                Log userLog = backend.getUserLog(logTxIdentifier);
                                Future<Message> env = userLog.add(txLogHeader.serializeModifications(serializer, LogTxStatus.USER_LOG, tx, addedRelations, deletedRelations));
                                if (env.isDone()) {
                                    try {
                                        env.get();
                                    } catch (ExecutionException ex) {
                                        throw ex.getCause();
                                    }
                                }
                                userlogSuccess = true;
                            } catch (Throwable e) {
                                status = LogTxStatus.SECONDARY_FAILURE;
                                LOG.error("Could not user-LOG committed transaction [" + transactionId + "] to " + logTxIdentifier, e);
                            }
                        }
                    } finally {
                        if (logTransaction) {
                            //[FAILURE] An exception here will be logged and not escalated; tx considered success and
                            // needs to be cleaned up later
                            try {
                                txLog.add(txLogHeader.serializeSecondary(serializer, status, indexFailures, userlogSuccess), txLogHeader.getLogKey());
                            } catch (Throwable e) {
                                LOG.error("Could not tx-LOG secondary persistence status on transaction [" + transactionId + "]", e);
                            }
                        }
                    }
                } else {
                    //This just closes the transaction since there are no modifications
                    mutator.commitIndexes();
                }
            } else { //Just commit everything at once
                //[FAILURE] This case only happens when there are no non-system mutations in which case all changes
                //are already flushed. Hence, an exception here is unlikely and should abort
                mutator.commit();
            }
        } catch (Throwable e) {
            LOG.error("Could not commit transaction [" + transactionId + "] due to exception", e);
            try {
                //Clean up any left-over transaction handles
                mutator.rollback();
            } catch (Throwable e2) {
                LOG.error("Could not roll-back transaction [" + transactionId + "] after failure due to exception", e2);
            }
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            else throw new JanusGraphException("Unexpected exception", e);
        }
    }
}
