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

package grakn.core.server.session;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import grakn.common.util.Pair;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.answer.Answer;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Explanation;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.Void;
import grakn.core.concept.impl.ConceptVertex;
import grakn.core.concept.impl.SchemaConceptImpl;
import grakn.core.concept.util.ConceptUtils;
import grakn.core.core.AttributeSerialiser;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.core.Schema;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.explanation.JoinExplanation;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.structure.GraknElementException;
import grakn.core.kb.concept.structure.PropertyNotUniqueException;
import grakn.core.kb.concept.structure.VertexElement;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.executor.QueryExecutor;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.cache.TransactionCache;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.keyspace.StatisticsDeltaImpl;
import grakn.core.server.Validator;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlCompute;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlQuery;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.MatchClause;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A TransactionOLTP that wraps a Tinkerpop OLTP transaction, using JanusGraph as a vendor backend.
 */
public class TransactionImpl implements Transaction {
    private final static Logger LOG = LoggerFactory.getLogger(TransactionImpl.class);
    private final long typeShardThreshold;

    // Shared Variables
    protected final Session  session;
    protected final ConceptManager conceptManager;
    protected final ExecutorFactory executorFactory;

    // Caches
    protected final MultilevelSemanticCache queryCache;
    protected final RuleCache ruleCache;
    protected final TransactionCache transactionCache;

    // TransactionOLTP Specific
    private final JanusGraphTransaction janusTransaction;
    protected final StatisticsDeltaImpl uncomittedStatisticsDelta;
    private Type txType;
    private String closedReason = null;
    private boolean isTxOpen;

    // Thread-local boolean which is set to true in the constructor. Used to check if current Tx is created in current Thread because
    // reaching across threads in a single threaded janus transaction leads to errors
    private final ThreadLocal<Boolean> createdInCurrentThread = ThreadLocal.withInitial(() -> Boolean.FALSE);

    protected final JanusTraversalSourceProvider janusTraversalSourceProvider;
    protected final ReasonerQueryFactory reasonerQueryFactory;
    private final ReadWriteLock graphLock;

    public TransactionImpl(Session session, JanusGraphTransaction janusTransaction, ConceptManager conceptManager,
                           JanusTraversalSourceProvider janusTraversalSourceProvider, TransactionCache transactionCache,
                           MultilevelSemanticCache queryCache, RuleCache ruleCache,
                           StatisticsDeltaImpl statisticsDelta, ExecutorFactory executorFactory,
                            ReasonerQueryFactory reasonerQueryFactory,
                           ReadWriteLock graphLock, long typeShardThreshold) {
        createdInCurrentThread.set(true);

        this.session = session;
        this.graphLock = graphLock;

        this.janusTransaction = janusTransaction;
        this.janusTraversalSourceProvider = janusTraversalSourceProvider;

        this.conceptManager = conceptManager;
        this.executorFactory = executorFactory;
        this.reasonerQueryFactory = reasonerQueryFactory;

        this.transactionCache = transactionCache;
        this.queryCache = queryCache;
        this.ruleCache = ruleCache;

        this.uncomittedStatisticsDelta = statisticsDelta;

        this.typeShardThreshold = typeShardThreshold;
    }

    @Override
    public void open(Type type) {
        this.txType = type;
        this.isTxOpen = true;
        this.transactionCache.updateSchemaCacheFromKeyspaceCache();
    }

    /**
     * This method handles the following committing scenarios:
     * - use a lock to serialise commits if two given txs try to insert the same attribute
     * - use a lock to serialise commits if two given txs try to insert a shard for the same type that
     * - use a lock if there is a tx that deletes attributes
     * - use a lock if there is a tx that mutates key implicit relations
     * - otherwise do not lock
     * @return true if graph lock need to be acquired for commit
     */
    @VisibleForTesting
    public boolean commitLockRequired(){
        String txId = this.janusTransaction.toString();
        boolean attributeLockRequired = session.attributeManager().requiresLock(txId);
        boolean shardLockRequired = session.shardManager().requiresLock(txId);
        boolean keyLockRequired = false;
        Set<String> modifiedKeyIndices = transactionCache.getModifiedKeyIndices();
        if (!modifiedKeyIndices.isEmpty()){
            Set<String> insertedIndices = transactionCache.getNewAttributes().keySet().stream().map(Pair::second).collect(Collectors.toSet());
            keyLockRequired = modifiedKeyIndices.stream().anyMatch(keyIndex -> !insertedIndices.contains(keyIndex));
        }
        boolean lockRequired = attributeLockRequired
                || shardLockRequired
                // In this case we need to lock, so that other concurrent Transactions
                // that are trying to create new attributes will read an updated version of attributesCache
                // Not locking here might lead to concurrent transactions reading the attributesCache that still
                // contains attributes that we are removing in this transaction.
                || !transactionCache.getRemovedAttributes().isEmpty()
                || keyLockRequired;
        if (lockRequired){
            LOG.debug(txId + " needs lock: " +
                    (attributeLockRequired? "attribute" : "") +
                    (shardLockRequired? "shard" : "") +
                    (keyLockRequired? "key" : "")+
                    (!transactionCache.getRemovedAttributes().isEmpty()? "delete" : ""));
        }
        return lockRequired;
    }

    private void commitInternal() throws InvalidKBException {
        boolean lockRequired = commitLockRequired();
        if (lockRequired) graphLock.writeLock().lock();
        try {
            createNewTypeShardsWhenThresholdReached();
            transactionCache.getRemovedAttributes().forEach(index -> session.attributeManager().attributesCommitted().invalidate(index));
            Set<String> deduplicatedIndices = mergeAttributes();
            persistInternal();
            ackCommit(deduplicatedIndices);

        } finally {
            if (lockRequired) graphLock.writeLock().unlock();
        }
    }

    private void persistInternal() throws InvalidKBException {
        validateGraph();
        session.keyspaceStatistics().commit(conceptManager, uncomittedStatisticsDelta);
        LOG.trace("Graph is valid. Committing graph...");
        janusTransaction.commit();
        LOG.trace("Graph committed.");
    }

    private void ackCommit(Set<String> deduplicatedIndices){
        String txId = this.janusTransaction.toString();
        session.shardManager().ackCommit(transactionCache.getNewShards().keySet(), txId);
        //this should ack all inserts so that insert requests are cleared
        Set<String> newIndices = transactionCache.getNewAttributes().keySet().stream().map(Pair::second).collect(Collectors.toSet());
        session.attributeManager().ackCommit(newIndices, txId);

        //this should ack all inserts that weren't deduplicated so that we have correct attributes in attributesCommitted
        transactionCache.getNewAttributes().forEach((indexPair, conceptId) -> {
            String index = indexPair.second();
            if (!deduplicatedIndices.contains(index)) session.attributeManager().attributesCommitted().put(index, conceptId);
        });

    }

    // When there are new attributes in the current transaction that is about to be committed
    // we serialise the commit by locking and merge attributes that are duplicates.
    private Set<String> mergeAttributes() {
        Set<String> deduplicatesIndices = new HashSet<>();
        transactionCache.getNewAttributes().forEach(((labelIndexPair, conceptId) -> {
            // If the same index is contained in attributesCommitted, it means
            // another concurrent transaction inserted the same attribute, time to merge!
            // NOTE: we still need to rely on attributesCommitted instead of checking in the graph
            // if the index exists, because apparently JanusGraph does not make indexes available
            // in a Read Committed fashion
            String index = labelIndexPair.second();
            Label label = labelIndexPair.first();
            ConceptId targetId = session.attributeManager().attributesCommitted().getIfPresent(index);
            if (targetId != null) {
                merge(conceptId, targetId);
                deduplicatesIndices.add(index);
                uncomittedStatisticsDelta.decrementAttribute(label);
            }
        }));
        return deduplicatesIndices;
    }

    @VisibleForTesting
    public void computeShardCandidates() {
        String txId = this.janusTransaction.toString();
        uncomittedStatisticsDelta.instanceDeltas().entrySet().stream()
                .filter(e -> !Schema.MetaSchema.isMetaLabel(e.getKey()))
                .forEach(e -> {
            Label label = e.getKey();
            Long uncommittedCount = e.getValue();
            long instanceCount = session.keyspaceStatistics().count(conceptManager, label) + uncommittedCount;
            long hardCheckpoint = getShardCheckpoint(label);
            if (instanceCount - hardCheckpoint >= typeShardThreshold) {
                session().shardManager().ackShardRequest(label, txId);
                //update cache to signal fulfillment of shard request later at commit time
                transactionCache.getNewShards().put(label, instanceCount);
            }
        });
    }

    private void createNewTypeShardsWhenThresholdReached() {
        String txId = this.janusTransaction.toString();
        transactionCache.getNewShards()
                .forEach((label, count) -> {
                    Long softCheckPoint = session.shardManager().getEphemeralShardCount(label);
                    long instanceCount = session.keyspaceStatistics().count(conceptManager, label) + uncomittedStatisticsDelta.delta(label);
                    if (softCheckPoint == null || instanceCount - softCheckPoint >= typeShardThreshold) {
                        session.shardManager().updateEphemeralShardCount(label, instanceCount);
                        LOG.trace(txId + " creates a shard for type: " + label + ", instance count: " + instanceCount + " ,");
                        shard(getType(label).id());
                        setShardCheckpoint(label, instanceCount);
                    }
        });
    }

    private void merge(ConceptId duplicateId, ConceptId targetId) {
        GraphTraversalSource tinkerTraversal = janusTraversalSourceProvider.getTinkerTraversal();
        Vertex duplicate = tinkerTraversal.V(Schema.elementId(duplicateId)).next();
        Vertex mergeTargetV = tinkerTraversal.V(Schema.elementId(targetId)).next();

        duplicate.vertices(Direction.IN).forEachRemaining(connectedVertex -> {
            // merge attribute edge connecting 'duplicate' and 'connectedVertex' to 'mergeTargetV', if exists
            GraphTraversal<Vertex, Edge> attributeEdge =
                    tinkerTraversal.V(duplicate).inE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).filter(__.outV().is(connectedVertex));
            if (attributeEdge.hasNext()) {
                mergeAttributeEdge(mergeTargetV, connectedVertex, attributeEdge);
            }

            // merge role-player edge connecting 'duplicate' and 'connectedVertex' to 'mergeTargetV', if exists
            GraphTraversal<Vertex, Edge> rolePlayerEdge =
                    tinkerTraversal.V(duplicate).inE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).filter(__.outV().is(connectedVertex));
            if (rolePlayerEdge.hasNext()) {
                mergeRolePlayerEdge(mergeTargetV, rolePlayerEdge);
            }
            try {
                attributeEdge.close();
                rolePlayerEdge.close();
            } catch (Exception e) {
                LOG.warn("Error closing the merging traversals", e);
            }
        });
        duplicate.remove();
    }

    private void mergeRolePlayerEdge(Vertex mergeTargetV, GraphTraversal<Vertex, Edge> rolePlayerEdge) {
        Edge edge = rolePlayerEdge.next();
        Vertex relationVertex = edge.outVertex();
        Object[] properties = propertiesToArray(Lists.newArrayList(edge.properties()));
        relationVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mergeTargetV, properties);
        edge.remove();
    }

    private void mergeAttributeEdge(Vertex mergeTargetV, Vertex ent, GraphTraversal<Vertex, Edge> attributeEdge) {
        Edge edge = attributeEdge.next();
        Object[] properties = propertiesToArray(Lists.newArrayList(edge.properties()));
        ent.addEdge(Schema.EdgeLabel.ATTRIBUTE.getLabel(), mergeTargetV, properties);
        edge.remove();
    }

    private Object[] propertiesToArray(ArrayList<Property<Object>> propertiesAsKeyValue) {
        ArrayList<Object> propertiesAsObj = new ArrayList<>();
        for (Property<Object> property : propertiesAsKeyValue) {
            propertiesAsObj.add(property.key());
            propertiesAsObj.add(property.value());
        }
        return propertiesAsObj.toArray();
    }

    @Override
    public Session session() {
        return session;
    }

    @Override
    public Keyspace keyspace() {
        return session.keyspace();
    }


    // Define Query

    @Override
    public List<ConceptMap> execute(GraqlDefine query) {
        return stream(query).collect(Collectors.toList());
    }

    @Override
    public Stream<ConceptMap> stream(GraqlDefine query) {
        checkMutationAllowed();
        return executorFactory.transactional( false).define(query);
    }

    // Undefine Query

    @Override
    public List<ConceptMap> execute(GraqlUndefine query) {
        return stream(query).collect(Collectors.toList());
    }

    @Override
    public Stream<ConceptMap> stream(GraqlUndefine query) {
        checkMutationAllowed();
        return executorFactory.transactional(false).undefine(query);
    }

    // Insert query

    @Override
    public List<ConceptMap> execute(GraqlInsert query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    @Override
    public List<ConceptMap> execute(GraqlInsert query) {
        return execute(query, true);
    }

    @Override
    public Stream<ConceptMap> stream(GraqlInsert query) {
        return stream(query, true);
    }


    @Override
    public Stream<ConceptMap> stream(GraqlInsert query, boolean infer) {
        checkMutationAllowed();
        Stream<ConceptMap> inserted = executor().insert(query);

        Stream<ConceptMap> explicitlyPersisted = inserted.peek(conceptMap -> {
            // mark all inferred concepts that are required for the insert for persistence explicitly
            // can avoid this potentially expensive check if there aren't any inferred concepts to start with
            if (transactionCache.getInferredInstances().findAny().isPresent()) {
                markConceptsForPersistence(conceptMap.concepts());
            }
        });

        return explicitlyPersisted;
    }

    /**
     * Mark all objects inserted explicitly for persistence, if they are inferred
     * The inferred objects also transitively check their generating concepts to be marked for persistence
     *
     * We do the persistence operation at the top level in the Transaction, because we have access to the Caches here
     * However, it may be better performed directly in the ConceptManager or elsewhere
     */
    private void markConceptsForPersistence(Collection<Concept> concepts) {
        Set<Thing> things = concepts.stream()
                .filter(Concept::isThing)
                .map(Concept::asThing)
                .collect(Collectors.toSet());

        ConceptUtils.getDependentConcepts(things)
                .filter(Thing::isInferred)
                .forEach(t -> {
                    //as we are going to persist the concepts, reset the inferred flag
                    ConceptVertex.from(t).vertex().property(Schema.VertexProperty.IS_INFERRED, false);
                   transactionCache.inferredInstanceToPersist(t);
                });
    }

    // Delete query

    @Override
    public List<Void> execute(GraqlDelete query) {
        return execute(query, true);
    }

    @Override
    public List<Void> execute(GraqlDelete query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    @Override
    public Stream<Void> stream(GraqlDelete query) {
        return stream(query, true);
    }

    @Override
    public Stream<Void> stream(GraqlDelete query, boolean infer) {
        checkMutationAllowed();
        return Stream.of(executor(infer).delete(query));
    }

    // Get Query

    @Override
    public List<ConceptMap> execute(GraqlGet query) {
        return execute(query, true);
    }

    @Override
    public List<ConceptMap> execute(GraqlGet query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    @Override
    public Stream<ConceptMap> stream(GraqlGet query) {
        return stream(query, true);
    }

    @Override
    public Stream<ConceptMap> stream(GraqlGet query, boolean infer) {
        return executor(infer).get(query);
    }

    // Aggregate Query

    @Override
    public List<Numeric> execute(GraqlGet.Aggregate query) {
        return execute(query, true);
    }

    @Override
    public List<Numeric> execute(GraqlGet.Aggregate query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    @Override
    public Stream<Numeric> stream(GraqlGet.Aggregate query) {
        return stream(query, true);
    }

    @Override
    public Stream<Numeric> stream(GraqlGet.Aggregate query, boolean infer) {
        return executor(infer).aggregate(query);
    }

    // Group Query

    @Override
    public List<AnswerGroup<ConceptMap>> execute(GraqlGet.Group query) {
        return execute(query, true);
    }

    @Override
    public List<AnswerGroup<ConceptMap>> execute(GraqlGet.Group query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    @Override
    public Stream<AnswerGroup<ConceptMap>> stream(GraqlGet.Group query) {
        return stream(query, true);
    }

    @Override
    public Stream<AnswerGroup<ConceptMap>> stream(GraqlGet.Group query, boolean infer) {
        return executor(infer).get(query);
    }

    // Group Aggregate Query

    @Override
    public List<AnswerGroup<Numeric>> execute(GraqlGet.Group.Aggregate query) {
        return execute(query, true);
    }

    @Override
    public List<AnswerGroup<Numeric>> execute(GraqlGet.Group.Aggregate query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    @Override
    public Stream<AnswerGroup<Numeric>> stream(GraqlGet.Group.Aggregate query) {
        return stream(query, true);
    }

    @Override
    public Stream<AnswerGroup<Numeric>> stream(GraqlGet.Group.Aggregate query, boolean infer) {
        return executor(infer).get(query);
    }

    // Compute Query

    @Override
    public List<Numeric> execute(GraqlCompute.Statistics query) {
        return stream(query).collect(Collectors.toList());
    }

    @Override
    public Stream<Numeric> stream(GraqlCompute.Statistics query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return executorFactory.compute().stream(query);
    }

    @Override
    public List<ConceptList> execute(GraqlCompute.Path query) {
        return stream(query).collect(Collectors.toList());
    }

    @Override
    public Stream<ConceptList> stream(GraqlCompute.Path query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return executorFactory.compute().stream(query);
    }

    @Override
    public List<ConceptSetMeasure> execute(GraqlCompute.Centrality query) {
        return stream(query).collect(Collectors.toList());
    }

    @Override
    public Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return executorFactory.compute().stream(query);
    }

    @Override
    public List<ConceptSet> execute(GraqlCompute.Cluster query) {
        return stream(query).collect(Collectors.toList());
    }

    @Override
    public Stream<ConceptSet> stream(GraqlCompute.Cluster query) {
        if (query.getException().isPresent()) throw query.getException().get();
        return executorFactory.compute().stream(query);
    }

    // Generic queries

    @Override
    public List<? extends Answer> execute(GraqlQuery query) {
        return execute(query, true);
    }

    @Override
    public List<? extends Answer> execute(GraqlQuery query, boolean infer) {
        return stream(query, infer).collect(Collectors.toList());
    }

    @Override
    public Stream<? extends Answer> stream(GraqlQuery query) {
        return stream(query, true);
    }

    @Override
    public Stream<? extends Answer> stream(GraqlQuery query, boolean infer) {
        if (query instanceof GraqlDefine) {
            return stream((GraqlDefine) query);

        } else if (query instanceof GraqlUndefine) {
            return stream((GraqlUndefine) query);

        } else if (query instanceof GraqlInsert) {
            return stream((GraqlInsert) query, infer);

        } else if (query instanceof GraqlDelete) {
            return stream((GraqlDelete) query, infer);

        } else if (query instanceof GraqlGet) {
            return stream((GraqlGet) query, infer);

        } else if (query instanceof GraqlGet.Aggregate) {
            return stream((GraqlGet.Aggregate) query, infer);

        } else if (query instanceof GraqlGet.Group.Aggregate) {
            return stream((GraqlGet.Group.Aggregate) query, infer);

        } else if (query instanceof GraqlGet.Group) {
            return stream((GraqlGet.Group) query, infer);

        } else if (query instanceof GraqlCompute.Statistics) {
            return stream((GraqlCompute.Statistics) query);

        } else if (query instanceof GraqlCompute.Path) {
            return stream((GraqlCompute.Path) query);

        } else if (query instanceof GraqlCompute.Centrality) {
            return stream((GraqlCompute.Centrality) query);

        } else if (query instanceof GraqlCompute.Cluster) {
            return stream((GraqlCompute.Cluster) query);

        } else {
            throw new IllegalArgumentException("Unrecognised Query object");
        }
    }

    @Override
    public boolean isOpen() {
        return isTxOpen;
    }

    @Override
    public Type type() {
        return this.txType;
    }


    //----------------------------------------------General Functionality-----------------------------------------------

    @Override
    public final Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        Set<SchemaConcept> superSet = new HashSet<>();

        while (schemaConcept != null) {
            superSet.add(schemaConcept);
            schemaConcept = schemaConcept.sup();
        }

        return superSet.stream();
    }

    public void checkMutationAllowed() {
        if (Type.READ.equals(type())) throw TransactionException.transactionReadOnly(this);
    }

    /**
     * Make sure graph is open and usable.
     *
     * @throws TransactionException if the graph is closed
     *                              or current transaction is not local (i.e. it was open in another thread)
     */
    private void checkGraphIsOpen() {
        if (!isLocal()) {
            throw TransactionException.notInOriginatingThread();
        }
        if (!isOpen()) {
            throw TransactionException.transactionClosed(this, this.closedReason);
        }
    }

    private boolean isLocal() {
        return createdInCurrentThread.get();
    }


    /**
     * @param label A unique label for the EntityType
     * @return A new or existing EntityType with the provided label
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-EntityType.
     */
    @Override
    public EntityType putEntityType(Label label) {
        checkGraphIsOpen();
        SchemaConceptImpl schemaConcept = conceptManager.getSchemaConcept(label);
        if (schemaConcept == null) {
            return conceptManager.createEntityType(label, getMetaEntityType());
        }

        ConceptUtils.validateBaseType(schemaConcept, Schema.BaseType.ENTITY_TYPE);

        return (EntityType) schemaConcept;
    }

    @Override
    public EntityType putEntityType(String label) {
        return putEntityType(Label.of(label));
    }


    /**
     * @param label A unique label for the RelationType
     * @return A new or existing RelationType with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-RelationType.
     */
    @Override
    public RelationType putRelationType(Label label) {
        checkGraphIsOpen();
        SchemaConceptImpl schemaConcept = conceptManager.getSchemaConcept(label);
        if (schemaConcept == null) {
            return conceptManager.createRelationType(label, getMetaRelationType());
        }

        ConceptUtils.validateBaseType(schemaConcept, Schema.BaseType.RELATION_TYPE);

        return (RelationType) schemaConcept;
    }

    @Override
    public RelationType putRelationType(String label) {
        return putRelationType(Label.of(label));
    }

    /**
     * @param label A unique label for the Role
     * @return new or existing Role with the provided Id.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Role.
     */
    @Override
    public Role putRole(Label label) {
        checkGraphIsOpen();
        SchemaConceptImpl schemaConcept = conceptManager.getSchemaConcept(label);
        if (schemaConcept == null) {
            return conceptManager.createRole(label, getMetaRole());
        }

        ConceptUtils.validateBaseType(schemaConcept, Schema.BaseType.ROLE);

        return (Role) schemaConcept;
    }

    @Override
    public Role putRole(String label) {
        return putRole(Label.of(label));
    }


    /**
     * @param label    A unique label for the AttributeType
     * @param dataType The data type of the AttributeType.
     *                 Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V>
     * @return A new or existing AttributeType with the provided label and data type.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-AttributeType.
     * @throws GraknElementException       if the {@param label} is already in use by an existing AttributeType which is
     *                                    unique or has a different datatype.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        checkGraphIsOpen();
        AttributeType<V> attributeType = conceptManager.getSchemaConcept(label);
        if (attributeType == null) {
            attributeType = conceptManager.createAttributeType(label, getMetaAttributeType(), dataType);
        } else {
            ConceptUtils.validateBaseType(SchemaConceptImpl.from(attributeType), Schema.BaseType.ATTRIBUTE_TYPE);
            //These checks is needed here because caching will return a type by label without checking the datatype

            if (Schema.MetaSchema.isMetaLabel(label)) {
                throw GraknConceptException.metaTypeImmutable(label);
            } else if (!dataType.equals(attributeType.dataType())) {
                throw GraknElementException.immutableProperty(attributeType.dataType(), dataType, Schema.VertexProperty.DATA_TYPE);
            }
        }

        return attributeType;
    }

    @Override
    public <V> AttributeType<V> putAttributeType(String label, AttributeType.DataType<V> dataType) {
        return putAttributeType(Label.of(label), dataType);
    }

    /**
     * @param label A unique label for the Rule
     * @param when
     * @param then
     * @return new or existing Rule with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Rule.
     */
    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        checkGraphIsOpen();
        Rule retrievedRule = conceptManager.getSchemaConcept(label);
        final Rule rule; // needed final for stream lambda
        if (retrievedRule == null) {
            rule = conceptManager.createRule(label, when, then, getMetaRule());
        } else {
            rule = retrievedRule;
            ConceptUtils.validateBaseType(SchemaConceptImpl.from(rule), Schema.BaseType.RULE);
        }

        ruleCache.ackRuleInsertion(rule);
        return rule;
    }

    @Override
    public Rule putRule(String label, Pattern when, Pattern then) {
        return putRule(Label.of(label), when, then);
    }


    //------------------------------------ Lookup

    /**
     * @param id  A unique identifier for the Concept in the graph.
     * @param <T>
     * @return The Concept with the provided id or null if no such Concept exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the concept is not an instance of T
     */
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        checkGraphIsOpen();
        return conceptManager.getConcept(id);
    }

    /**
     * Get the root of all Types.
     *
     * @return The meta type -> type.
     */
    @Override
    @CheckReturnValue
    public grakn.core.kb.concept.api.Type getMetaConcept() {
        checkGraphIsOpen();
        return conceptManager.getMetaConcept();
    }

    /**
     * Get the root of all RelationType.
     *
     * @return The meta relation type -> relation-type.
     */
    @Override
    @CheckReturnValue
    public RelationType getMetaRelationType() {
        checkGraphIsOpen();
        return conceptManager.getMetaRelationType();
    }

    /**
     * Get the root of all the Role.
     *
     * @return The meta role type -> role-type.
     */
    @Override
    @CheckReturnValue
    public Role getMetaRole() {
        checkGraphIsOpen();
        return conceptManager.getMetaRole();
    }

    /**
     * Get the root of all the AttributeType.
     *
     * @return The meta resource type -> resource-type.
     */
    @Override
    @CheckReturnValue
    public AttributeType getMetaAttributeType() {
        checkGraphIsOpen();
        return conceptManager.getMetaAttributeType();
    }

    /**
     * Get the root of all the Entity Types.
     *
     * @return The meta entity type -> entity-type.
     */
    @Override
    @CheckReturnValue
    public EntityType getMetaEntityType() {
        checkGraphIsOpen();
        return conceptManager.getMetaEntityType();
    }

    /**
     * Get the root of all Rules;
     *
     * @return The meta Rule
     */
    @Override
    @CheckReturnValue
    public Rule getMetaRule() {
        checkGraphIsOpen();
        return conceptManager.getMetaRule();
    }


    /**
     * @param value A value which an Attribute in the graph may be holding.
     * @param <V>
     * @return The Attributes holding the provided value or an empty collection if no such Attribute exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        checkGraphIsOpen();
        if (value == null) return Collections.emptySet();

        // TODO: Remove this forced casting once we replace DataType to be Parameterised Generic Enum
        AttributeType.DataType<V> dataType =
                (AttributeType.DataType<V>) AttributeType.DataType.of(value.getClass());
        if (dataType == null) {
            throw TransactionException.unsupportedDataType(value);
        }

        HashSet<Attribute<V>> attributes = new HashSet<>();
        conceptManager.getConcepts(Schema.VertexProperty.ofDataType(dataType), AttributeSerialiser.of(dataType).serialise(value))
                .forEach(concept -> {
                    if (concept != null && concept.isAttribute()) {
                        attributes.add(concept.asAttribute());
                    }
                });

        return attributes;
    }

    /**
     * @param label A unique label which identifies the SchemaConcept in the graph.
     * @param <T>
     * @return The SchemaConcept with the provided label or null if no such SchemaConcept exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of T
     */
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        checkGraphIsOpen();
        return conceptManager.getSchemaConcept(label);
    }

    /**
     * @param label A unique label which identifies the grakn.core.kb.concept.api.Type in the graph.
     * @param <T>
     * @return The grakn.core.kb.concept.api.Type with the provided label or null if no such grakn.core.kb.concept.api.Type exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of T
     */
    @Override
    public <T extends grakn.core.kb.concept.api.Type> T getType(Label label) {
        checkGraphIsOpen();
        return conceptManager.getType(label);
    }

    /**
     * @param label A unique label which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided label or null if no such Entity Type exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public EntityType getEntityType(String label) {
        checkGraphIsOpen();
        return conceptManager.getEntityType(label);
    }

    /**
     * @param label A unique label which identifies the RelationType in the graph.
     * @return The RelationType with the provided label or null if no such RelationType exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public RelationType getRelationType(String label) {
        checkGraphIsOpen();
        return conceptManager.getRelationType(label);
    }

    /**
     * @param label A unique label which identifies the AttributeType in the graph.
     * @param <V>
     * @return The AttributeType with the provided label or null if no such AttributeType exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        checkGraphIsOpen();
        return conceptManager.getAttributeType(label);
    }

    /**
     * @param label A unique label which identifies the Role Type in the graph.
     * @return The Role Type  with the provided label or null if no such Role Type exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public Role getRole(String label) {
        checkGraphIsOpen();
        return conceptManager.getRole(label);
    }

    /**
     * @param label A unique label which identifies the Rule in the graph.
     * @return The Rule with the provided label or null if no such Rule Type exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public Rule getRule(String label) {
        checkGraphIsOpen();
        return conceptManager.getRule(label);
    }

    @Override
    public Explanation explanation(Pattern queryPattern) {
        GraqlGet getQuery = Graql.match(queryPattern).get();
        ResolvableQuery q = reasonerQueryFactory.resolvable(Iterables.getOnlyElement(getQuery.match().getPatterns().getNegationDNF().getPatterns()));

        Explanation explanation;
        if (q.isAtomic()) {
            // If the query is atomic, looking up the query in the cache will result in retrieving the answer associated with the query
            // we then return the answer's explanation
            // Only atomic queries are retrievable directly from the cache
            ReasonerAtomicQuery asAtomicQuery = reasonerQueryFactory.atomic(q.selectAtoms().findFirst().get());
            ConceptMap originatingAnswer = queryCache.getAnswerStream(asAtomicQuery).findFirst().orElse(null);
            if (originatingAnswer == null) { throw ReasonerException.queryCacheAnswerNotFound(getQuery); }
            explanation = originatingAnswer.explanation();
        } else {
            // If the query is not atomic, we can break it down into sub queries and retrieve each component's answer
            // these are the same components used to re-construct the explanation for our original query, which we
            // whenever we set the pattern, we need to provide the correct variable ID substitutions as well
            List<ConceptMap> maps = q.selectAtoms()
                    .map(atom -> reasonerQueryFactory.atomic(atom))
                    .flatMap(aq -> {
                        Stream<ConceptMap> answerStream = queryCache.getAnswerStream(aq);
                        return answerStream.map(conceptMap -> conceptMap.withPattern(aq.withSubstitution(conceptMap).getPattern()));
                    })
                    .collect(Collectors.toList());
            explanation = new JoinExplanation(maps);
        }

        return explanation;
    }

    @Override
    public void close() {
        close(ErrorMessage.TX_CLOSED.getMessage(keyspace()));
    }

    /**
     * Close the transaction without committing
     */
    @Override
    public void close(String closeMessage) {
        if (!isOpen()) {
            return;
        }
        try {
            if (janusTransaction.isOpen()) {
                janusTransaction.rollback();
            }
        } finally {
            closeTransaction(closeMessage);
        }
    }

    /**
     * Commits and closes the transaction
     *
     * @throws InvalidKBException if graph does not comply with the grakn validation rules
     */
    @Override
    public void commit() throws InvalidKBException {
        if (!isOpen()) {
            return;
        }
        try {
            checkMutationAllowed();
            removeInferredConcepts();
            computeShardCandidates();

            // lock on the keyspace cache shared between concurrent tx's to the same keyspace
            // force serialized updates, keeping Janus and our KeyspaceCache in sync
            commitInternal();
            transactionCache.flushSchemaLabelIdsToCache();
        } finally {
            String closeMessage = ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("committed", keyspace());
            closeTransaction(closeMessage);
        }
    }

    private void closeTransaction(String closedReason) {
        this.closedReason = closedReason;
        this.isTxOpen = false;
        ruleCache.clear();
        queryCache.clear();
    }

    private void removeInferredConcepts() {
        Set<Thing> inferredThingsToDiscard = transactionCache.getInferredInstancesToDiscard().collect(Collectors.toSet());
        inferredThingsToDiscard.forEach(transactionCache::remove);
        inferredThingsToDiscard.forEach(Concept::delete);
    }

    private void validateGraph() throws InvalidKBException {
        Validator validator = new Validator(reasonerQueryFactory, transactionCache, conceptManager);
        if (!validator.validate()) {
            List<String> errors = validator.getErrorsFound();
            if (!errors.isEmpty()) throw InvalidKBException.validationErrors(errors);
        }
    }

    /**
     * Creates a new shard for the concept - only used in tests
     *
     * @param conceptId the id of the concept to shard
     */
    public void shard(ConceptId conceptId) {
        Concept type = getConcept(conceptId);
        if (type == null) {
            throw new RuntimeException("Cannot shard concept [" + conceptId + "] due to it not existing in the graph");
        }
        if (type.isType()) {
            type.asType().createShard();
        } else {
            throw GraknConceptException.cannotShard(type);
        }
    }

    /**
     * Set a new type shard checkpoint for a given label
     */
    private void setShardCheckpoint(Label label, long checkpoint) {
        Concept schemaConcept = getSchemaConcept(label);
        if (schemaConcept != null) {
            Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
            janusVertex.property(Schema.VertexProperty.TYPE_SHARD_CHECKPOINT.name(), checkpoint);
        } else {
            throw new RuntimeException("Label '" + label.getValue() + "' does not exist");
        }
    }

    /**
     * Get the checkpoint in which type shard was last created
     *
     * @param label
     * @return the checkpoint for the given label. if the label does not exist, return 0
     */
    private Long getShardCheckpoint(Label label) {
        Concept schemaConcept = getSchemaConcept(label);
        if (schemaConcept != null) {
            Vertex janusVertex = ConceptVertex.from(schemaConcept).vertex().element();
            VertexProperty<Object> property = janusVertex.property(Schema.VertexProperty.TYPE_SHARD_CHECKPOINT.name());
            return (Long) property.orElse(0L);
        } else {
            return 0L;
        }
    }


    @Override
    public Stream<ConceptMap> stream(MatchClause matchClause) {
        return executor().match(matchClause);
    }

    @Override
    public Stream<ConceptMap> stream(MatchClause matchClause, boolean infer) {
        return executor(infer).match(matchClause);
    }

    @Override
    public List<ConceptMap> execute(MatchClause matchClause) {
        return stream(matchClause).collect(Collectors.toList());
    }

    @Override
    public List<ConceptMap> execute(MatchClause matchClause, boolean infer) {
        return stream(matchClause, infer).collect(Collectors.toList());
    }


    // shortcut helpers
    private QueryExecutor executor() {
        return executorFactory.transactional( true);
    }

    private QueryExecutor executor(boolean infer) {
        return executorFactory.transactional( infer);
    }

    // ----------- Exposed low level methods that should not be exposed here TODO refactor
    void createMetaConcepts() {
        VertexElement type = conceptManager.addTypeVertex(Schema.MetaSchema.THING.getId(), Schema.MetaSchema.THING.getLabel(), Schema.BaseType.TYPE);
        VertexElement entityType = conceptManager.addTypeVertex(Schema.MetaSchema.ENTITY.getId(), Schema.MetaSchema.ENTITY.getLabel(), Schema.BaseType.ENTITY_TYPE);
        VertexElement relationType = conceptManager.addTypeVertex(Schema.MetaSchema.RELATION.getId(), Schema.MetaSchema.RELATION.getLabel(), Schema.BaseType.RELATION_TYPE);
        VertexElement resourceType = conceptManager.addTypeVertex(Schema.MetaSchema.ATTRIBUTE.getId(), Schema.MetaSchema.ATTRIBUTE.getLabel(), Schema.BaseType.ATTRIBUTE_TYPE);
        conceptManager.addTypeVertex(Schema.MetaSchema.ROLE.getId(), Schema.MetaSchema.ROLE.getLabel(), Schema.BaseType.ROLE);
        conceptManager.addTypeVertex(Schema.MetaSchema.RULE.getId(), Schema.MetaSchema.RULE.getLabel(), Schema.BaseType.RULE);

        relationType.property(Schema.VertexProperty.IS_ABSTRACT, true);
        resourceType.property(Schema.VertexProperty.IS_ABSTRACT, true);
        entityType.property(Schema.VertexProperty.IS_ABSTRACT, true);

        relationType.addEdge(type, Schema.EdgeLabel.SUB);
        resourceType.addEdge(type, Schema.EdgeLabel.SUB);
        entityType.addEdge(type, Schema.EdgeLabel.SUB);
    }


}