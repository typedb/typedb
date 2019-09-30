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
 */

package grakn.core.server.session;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import grakn.benchmark.lib.instrumentation.ServerTracing;
import grakn.core.api.Transaction;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.LabelId;
import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.graql.executor.QueryExecutor;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.PropertyNotUniqueException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.Validator;
import grakn.core.server.kb.concept.ConceptImpl;
import grakn.core.server.kb.concept.ConceptManager;
import grakn.core.server.kb.concept.ConceptVertex;
import grakn.core.server.kb.concept.SchemaConceptImpl;
import grakn.core.server.kb.concept.Serialiser;
import grakn.core.server.kb.concept.TypeImpl;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.cache.CacheProvider;
import grakn.core.server.session.cache.RuleCache;
import grakn.core.server.session.cache.TransactionCache;
import grakn.core.server.statistics.UncomittedStatisticsDelta;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlCompute;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.MatchClause;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A TransactionOLTP that wraps a Tinkerpop OLTP transaction, using JanusGraph as a vendor backend.
 */
public class TransactionOLTP implements Transaction {
    private final static Logger LOG = LoggerFactory.getLogger(TransactionOLTP.class);
    private final long typeShardThreshold;

    // Shared Variables
    private final SessionImpl session;
    private final ConceptManager conceptManager;

    // Caches
    private final MultilevelSemanticCache queryCache;
    private final RuleCache ruleCache;
    private final TransactionCache transactionCache;

    // TransactionOLTP Specific
    private final JanusGraphTransaction janusTransaction;
    private Transaction.Type txType;
    private String closedReason = null;
    private boolean isTxOpen;
    private UncomittedStatisticsDelta uncomittedStatisticsDelta;

    // Thread-local boolean which is set to true in the constructor. Used to check if current Tx is created in current Thread.
    private final ThreadLocal<Boolean> createdInCurrentThread = ThreadLocal.withInitial(() -> Boolean.FALSE);

    @Nullable
    private GraphTraversalSource graphTraversalSource = null;

    public static class Builder implements Transaction.Builder {

        private SessionImpl session;

        Builder(SessionImpl session) {
            this.session = session;
        }

        @Override
        public TransactionOLTP read() {
            return session.transaction(Transaction.Type.READ);
        }

        @Override
        public TransactionOLTP write() {
            return session.transaction(Transaction.Type.WRITE);
        }
    }

    public TransactionOLTP(SessionImpl session, JanusGraphTransaction janusTransaction, ConceptManager conceptManager,
                           CacheProvider cacheProvider, UncomittedStatisticsDelta statisticsDelta) {
        createdInCurrentThread.set(true);

        this.session = session;

        this.janusTransaction = janusTransaction;

        this.conceptManager = conceptManager;

        this.transactionCache = cacheProvider.getTransactionCache();
        this.queryCache = cacheProvider.getQueryCache();
        this.ruleCache = cacheProvider.getRuleCache();
        // TODO remove temporal coupling
        this.ruleCache.setTx(this);

        this.uncomittedStatisticsDelta = statisticsDelta;

        typeShardThreshold = this.session.config().getProperty(ConfigKey.TYPE_SHARD_THRESHOLD);
    }

    public void open(Type type) {
        this.txType = type;
        this.isTxOpen = true;
        this.transactionCache.updateSchemaCacheFromKeyspaceCache();
    }


    /**
     * This method handles 3 committing scenarios:
     * - use a lock to serialise all commits that are trying to create new attributes, so that we can merge real-time
     * - use a lock to serialise commits that are removing attributes, so concurrent txs don't use outdated attribute IDs from attributesCache
     * - don't lock when added or removed attributes are not involved
     */
    private void commitInternal() {
        LOG.trace("Graph is valid. Committing graph...");
        if (!cache().getNewAttributes().isEmpty()) {
            mergeAttributesAndCommit();
        } else if (!cache().getRemovedAttributes().isEmpty()) {
            // In this case we need to lock, so that other concurrent Transactions
            // that are trying to create new attributes will read an updated version of attributesCache
            // Not locking here might lead to concurrent transactions reading the attributesCache that still
            // contains attributes that we are removing in this transaction.
            session.graphLock().writeLock().lock();
            try {
                createNewTypeShardsWhenThresholdReached();
                session.keyspaceStatistics().commit(this, uncomittedStatisticsDelta);
                janusTransaction.commit();
                cache().getRemovedAttributes().forEach(index -> session.attributesCache().invalidate(index));
            } finally {
                session.graphLock().writeLock().unlock();
            }
        } else {
            createNewTypeShardsWhenThresholdReached();
            session.keyspaceStatistics().commit(this, uncomittedStatisticsDelta);
            janusTransaction.commit();
        }
        LOG.trace("Graph committed.");
    }

    // When there are new attributes in the current transaction that is about to be committed
    // we serialise the commit by locking and merge attributes that are duplicates.
    private void mergeAttributesAndCommit() {
        session.graphLock().writeLock().lock();
        try {
            createNewTypeShardsWhenThresholdReached();
            cache().getRemovedAttributes().forEach(index -> session.attributesCache().invalidate(index));
            cache().getNewAttributes().forEach(((labelIndexPair, conceptId) -> {
                // If the same index is contained in attributesCache, it means
                // another concurrent transaction inserted the same attribute, time to merge!
                // NOTE: we still need to rely on attributesCache instead of checking in the graph
                // if the index exists, because apparently JanusGraph does not make indexes available
                // in a Read Committed fashion
                ConceptId targetId = session.attributesCache().getIfPresent(labelIndexPair.getValue());
                if (targetId != null) {
                    merge(getTinkerTraversal(), conceptId, targetId);
                    statisticsDelta().decrement(labelIndexPair.getKey());
                } else {
                    session.attributesCache().put(labelIndexPair.getValue(), conceptId);
                }
            }));
            session.keyspaceStatistics().commit(this, uncomittedStatisticsDelta);
            janusTransaction.commit();
        } finally {
            session.graphLock().writeLock().unlock();
        }
    }

    private void createNewTypeShardsWhenThresholdReached() {
        uncomittedStatisticsDelta.instanceDeltas().forEach((label, uncommittedCount) -> {
            long instancesCount = session.keyspaceStatistics().count(this, label) + uncomittedStatisticsDelta.instanceDeltas().get(label) + uncommittedCount;
            long lastShardCheckpointForThisInstance = getShardCheckpoint(label);
            if (instancesCount - lastShardCheckpointForThisInstance >= typeShardThreshold) {
                LOG.trace(label + " has a count of " + instancesCount + ". last sharding happens at " + lastShardCheckpointForThisInstance + ". Will create a new shard.");
                shard(getType(label).id());
                setShardCheckpoint(label, instancesCount);
            }
        });
    }

    private static void merge(GraphTraversalSource tinkerTraversal, ConceptId duplicateId, ConceptId targetId) {
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

    private static void mergeRolePlayerEdge(Vertex mergeTargetV, GraphTraversal<Vertex, Edge> rolePlayerEdge) {
        Edge edge = rolePlayerEdge.next();
        Vertex relationVertex = edge.outVertex();
        Object[] properties = propertiesToArray(Lists.newArrayList(edge.properties()));
        relationVertex.addEdge(Schema.EdgeLabel.ROLE_PLAYER.getLabel(), mergeTargetV, properties);
        edge.remove();
    }

    private static void mergeAttributeEdge(Vertex mergeTargetV, Vertex ent, GraphTraversal<Vertex, Edge> attributeEdge) {
        Edge edge = attributeEdge.next();
        Object[] properties = propertiesToArray(Lists.newArrayList(edge.properties()));
        ent.addEdge(Schema.EdgeLabel.ATTRIBUTE.getLabel(), mergeTargetV, properties);
        edge.remove();
    }

    private static Object[] propertiesToArray(ArrayList<Property<Object>> propertiesAsKeyValue) {
        ArrayList<Object> propertiesAsObj = new ArrayList<>();
        for (Property<Object> property : propertiesAsKeyValue) {
            propertiesAsObj.add(property.key());
            propertiesAsObj.add(property.value());
        }
        return propertiesAsObj.toArray();
    }

    @Override
    public SessionImpl session() {
        return session;
    }

    @Override
    public KeyspaceImpl keyspace() {
        return session.keyspace();
    }

    @Override
    public Stream<ConceptMap> stream(GraqlDefine query) {
        return Stream.of(executor().define(query));
    }

    @Override
    public Stream<ConceptMap> stream(GraqlUndefine query) {
        return Stream.of(executor().undefine(query));
    }

    @Override
    public Stream<ConceptMap> stream(GraqlInsert query, boolean infer) {
        return executor().insert(query);
    }

    @Override
    public Stream<ConceptSet> stream(GraqlDelete query, boolean infer) {
        return Stream.of(executor(infer).delete(query));
    }

    @Override
    public Stream<ConceptMap> stream(GraqlGet query, boolean infer) {
        return executor(infer).get(query);
    }

    @Override
    public Stream<Numeric> stream(GraqlGet.Aggregate query, boolean infer) {
        return executor(infer).aggregate(query);
    }

    @Override
    public Stream<AnswerGroup<ConceptMap>> stream(GraqlGet.Group query, boolean infer) {
        return executor(infer).get(query);
    }

    @Override
    public Stream<AnswerGroup<Numeric>> stream(GraqlGet.Group.Aggregate query, boolean infer) {
        return executor(infer).get(query);
    }

    @Override
    public Stream<Numeric> stream(GraqlCompute.Statistics query) {
        return executor(false).compute(query);
    }

    @Override
    public Stream<ConceptList> stream(GraqlCompute.Path query) {
        return executor(false).compute(query);
    }

    @Override
    public Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query) {
        return executor(false).compute(query);
    }

    @Override
    public Stream<ConceptSet> stream(GraqlCompute.Cluster query) {
        return executor(false).compute(query);
    }

    public RuleCache ruleCache() {
        return ruleCache;
    }

    public MultilevelSemanticCache queryCache() {
        return queryCache;
    }


    /**
     * Gets the config option which determines the number of instances a grakn.core.concept.type.Type must have before the grakn.core.concept.type.Type
     * if automatically sharded.
     *
     * @return the number of instances a grakn.core.concept.type.Type must have before it is shareded
     */
    public long shardingThreshold() {
        return typeShardThreshold;
    }

    public TransactionCache cache() {
        return transactionCache;
    }

    public UncomittedStatisticsDelta statisticsDelta() {
        return uncomittedStatisticsDelta;
    }

    @Override
    public boolean isClosed() {
        return !isTxOpen;
    }

    @Override
    public Type type() {
        return this.txType;
    }

    /**
     * Utility function to get a read-only Tinkerpop traversal.
     *
     * @return A read-only Tinkerpop traversal for manually traversing the graph
     * <p>
     * Mostly used for tests // TODO refactor push this implementation down from Transaction itself, should not be here
     */
    public GraphTraversalSource getTinkerTraversal() {
        checkGraphIsOpen();
        if (graphTraversalSource == null) {
            graphTraversalSource = janusTransaction.traversal().withStrategies(ReadOnlyStrategy.instance());
        }
        return graphTraversalSource;
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
        if (!isLocal() || isClosed()) throw TransactionException.transactionClosed(this, this.closedReason);
    }

    private boolean isLocal() {
        return createdInCurrentThread.get();
    }

    private void validateBaseType(SchemaConceptImpl schemaConcept, Schema.BaseType expectedBaseType) {
        // extra validation to ensure schema integrity -- is this needed?
        // throws if label is already taken for a different type
        if (!expectedBaseType.equals(schemaConcept.baseType())) {
            throw PropertyNotUniqueException.cannotCreateProperty(schemaConcept, Schema.VertexProperty.SCHEMA_LABEL, schemaConcept.label());
        }
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

        validateBaseType(schemaConcept, Schema.BaseType.ENTITY_TYPE);

        return (EntityType) schemaConcept;
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

        validateBaseType(schemaConcept, Schema.BaseType.RELATION_TYPE);

        return (RelationType) schemaConcept;
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

        validateBaseType(schemaConcept, Schema.BaseType.ROLE);

        return (Role) schemaConcept;
    }


    /**
     * @param label    A unique label for the AttributeType
     * @param dataType The data type of the AttributeType.
     *                 Supported types include: DataType.STRING, DataType.LONG, DataType.DOUBLE, and DataType.BOOLEAN
     * @param <V>
     * @return A new or existing AttributeType with the provided label and data type.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-AttributeType.
     * @throws TransactionException       if the {@param label} is already in use by an existing AttributeType which is
     *                                    unique or has a different datatype.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        checkGraphIsOpen();
        AttributeType<V> attributeType = conceptManager.getSchemaConcept(label);
        if (attributeType == null) {
            attributeType = conceptManager.createAttributeType(label, getMetaAttributeType(), dataType);
        } else {
            validateBaseType(SchemaConceptImpl.from(attributeType), Schema.BaseType.ATTRIBUTE_TYPE);
            //These checks is needed here because caching will return a type by label without checking the datatype

            if (Schema.MetaSchema.isMetaLabel(label)) {
                throw TransactionException.metaTypeImmutable(label);
            } else if (!dataType.equals(attributeType.dataType())) {
                throw TransactionException.immutableProperty(attributeType.dataType(), dataType, Schema.VertexProperty.DATA_TYPE);
            }
        }

        return attributeType;
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
            validateBaseType(SchemaConceptImpl.from(rule), Schema.BaseType.RULE);
        }

        //NB: thenTypes() will be empty as type edges added on commit
        //NB: this will cache also non-committed rules
        if (rule.then() != null) {
            rule.then().statements().stream()
                    .flatMap(v -> v.getTypes().stream())
                    .map(type -> this.<SchemaConcept>getSchemaConcept(Label.of(type)))
                    .filter(Objects::nonNull)
                    .filter(Concept::isType)
                    .map(Concept::asType)
                    .forEach(type -> ruleCache.updateRules(type, rule));
        }
        return rule;
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
        conceptManager.getConcepts(Schema.VertexProperty.ofDataType(dataType), Serialiser.of(dataType).serialise(value))
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
        Schema.MetaSchema meta = Schema.MetaSchema.valueOf(label);
        if (meta != null) {
            return conceptManager.getSchemaConcept(meta.getId());
        } else {
            return conceptManager.getSchemaConcept(label, Schema.BaseType.SCHEMA_CONCEPT);
        }
    }

    /**
     * @param label A unique label which identifies the grakn.core.concept.type.Type in the graph.
     * @param <T>
     * @return The grakn.core.concept.type.Type with the provided label or null if no such grakn.core.concept.type.Type exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the type is not an instance of T
     */
    @Override
    public <T extends grakn.core.concept.type.Type> T getType(Label label) {
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
    public void close() {
        close(ErrorMessage.TX_CLOSED.getMessage(keyspace()));
    }

    /**
     * Close the transaction without committing
     */

    void close(String closeMessage) {
        if (isClosed()) {
            return;
        }
        try {
            janusTransaction.close();
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
        if (isClosed()) {
            return;
        }
        try {
            /* This method has permanent tracing because commits can take varying lengths of time depending on operations */
            int validateSpanId = ServerTracing.startScopedChildSpan("commit validate");

            checkMutationAllowed();
            removeInferredConcepts();
            validateGraph();

            ServerTracing.closeScopedChildSpan(validateSpanId);

            int commitSpanId = ServerTracing.startScopedChildSpan("commit");

            // lock on the keyspace cache shared between concurrent tx's to the same keyspace
            // force serialized updates, keeping Janus and our KeyspaceCache in sync
            commitInternal();
            transactionCache.flushSchemaLabelIdsToCache();

            ServerTracing.closeScopedChildSpan(commitSpanId);
        } finally {
            String closeMessage = ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("committed", keyspace());
            closeTransaction(closeMessage);
        }
    }

    private void closeTransaction(String closedReason) {
        this.closedReason = closedReason;
        this.isTxOpen = false;
        ruleCache().clear();
        queryCache().clear();
    }

    private void removeInferredConcepts() {
        Set<Thing> inferredThingsToDiscard = cache().getInferredInstancesToDiscard().collect(Collectors.toSet());
        inferredThingsToDiscard.forEach(inferred -> cache().remove(inferred));
        inferredThingsToDiscard.forEach(Concept::delete);
    }

    private void validateGraph() throws InvalidKBException {
        Validator validator = new Validator(this);
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
        ConceptImpl type = getConcept(conceptId);
        if (type == null) {
            throw new RuntimeException("Cannot shard concept [" + conceptId + "] due to it not existing in the graph");
        }
        type.createShard();
    }

    /**
     * Set a new type shard checkpoint for a given label
     *
     * @param label
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

    /**
     * Returns the current number of shards the provided grakn.core.concept.type.Type has. This is used in creating more
     * efficient query plans.
     *
     * @param concept The grakn.core.concept.type.Type which may contain some shards.
     * @return the number of Shards the grakn.core.concept.type.Type currently has.
     */
    public long getShardCount(grakn.core.concept.type.Type concept) {
        return TypeImpl.from(concept).shardCount();
    }

    public final QueryExecutor executor() {
        return new QueryExecutor(this, conceptManager, true);
    }

    public final QueryExecutor executor(boolean infer) {
        return new QueryExecutor(this, conceptManager, infer);
    }

    public Stream<ConceptMap> stream(MatchClause matchClause) {
        return executor().match(matchClause);
    }

    public Stream<ConceptMap> stream(MatchClause matchClause, boolean infer) {
        return executor(infer).match(matchClause);
    }

    public List<ConceptMap> execute(MatchClause matchClause) {
        return stream(matchClause).collect(Collectors.toList());
    }

    public List<ConceptMap> execute(MatchClause matchClause, boolean infer) {
        return stream(matchClause, infer).collect(Collectors.toList());
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

    public LabelId convertToId(Label label) {
        return conceptManager.convertToId(label);
    }

    @VisibleForTesting
    public ConceptManager factory() {
        return conceptManager;
    }

}