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
import grakn.core.server.kb.concept.ElementFactory;
import grakn.core.server.kb.concept.SchemaConceptImpl;
import grakn.core.server.kb.concept.Serialiser;
import grakn.core.server.kb.concept.TypeImpl;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.cache.KeyspaceCache;
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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A TransactionOLTP that wraps a Tinkerpop OLTP transaction, using JanusGraph as a vendor backend.
 */
public class TransactionOLTP implements Transaction {
    private final static Logger LOG = LoggerFactory.getLogger(TransactionOLTP.class);
    // Shared Variables
    private final SessionImpl session;
    private final ElementFactory elementFactory;

    // Caches
    private final MultilevelSemanticCache queryCache;
    private final RuleCache ruleCache;
    private final KeyspaceCache keyspaceCache;
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

    TransactionOLTP(SessionImpl session, JanusGraphTransaction janusTransaction, KeyspaceCache keyspaceCache) {
        createdInCurrentThread.set(true);

        this.session = session;

        this.janusTransaction = janusTransaction;

        this.elementFactory = new ElementFactory(this);

        this.queryCache = new MultilevelSemanticCache();
        this.ruleCache = new RuleCache(this);

        this.keyspaceCache = keyspaceCache;
        this.transactionCache = new TransactionCache(keyspaceCache);

        this.uncomittedStatisticsDelta = new UncomittedStatisticsDelta();

    }

    void open(Type type) {
        this.txType = type;
        this.isTxOpen = true;
        this.transactionCache.updateSchemaCacheFromKeyspaceCache();
    }


    /**
     * This method handles 3 committing scenarios:
     * - use a lock to serialise all commits that are trying to create new attributes, so that we can merge real-time
     * - use a lock to serialise commits that are removing attributes, so concurrent txs dont use outdate attribute IDs from attributesCache
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
                session.keyspaceStatistics().commit(this, uncomittedStatisticsDelta);
                janusTransaction.commit();
                cache().getRemovedAttributes().forEach(index -> session.attributesCache().invalidate(index));
            } finally {
                session.graphLock().writeLock().unlock();
            }
        } else {
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

    /**
     * Creates a new Vertex in the graph and builds a VertexElement which wraps the newly created vertex
     *
     * @param baseType baseType of newly created Vertex
     * @return VertexElement
     */
    public VertexElement addVertexElement(Schema.BaseType baseType) {
        Vertex vertex = janusTransaction.addVertex(baseType.name());
        return factory().buildVertexElement(vertex);
    }

    /**
     * This is only used when reifying a Relation, creates a new Vertex in the graph representing the reified relation.
     * NB: this is only called when we reify an EdgeRelation - we want to preserve the ID property of the concept
     *
     * @param baseType  Concept BaseType which will become the VertexLabel
     * @param conceptId ConceptId to be set on the vertex
     * @return just created Vertex
     */
    public VertexElement addVertexElementWithEdgeIdProperty(Schema.BaseType baseType, ConceptId conceptId) {
        Vertex vertex = janusTransaction.addVertex(baseType.name());
        vertex.property(Schema.VertexProperty.EDGE_RELATION_ID.name(), conceptId.getValue());
        return factory().buildVertexElement(vertex);
    }

    public boolean isValidElement(Element element) {
        return element != null && !((JanusGraphElement) element).isRemoved();
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
     * Converts a Type Label into a type Id for this specific graph. Mapping labels to ids will differ between graphs
     * so be sure to use the correct graph when performing the mapping.
     *
     * @param label The label to be converted to the id
     * @return The matching type id
     */
    public LabelId convertToId(Label label) {
        if (transactionCache.isLabelCached(label)) {
            return transactionCache.convertLabelToId(label);
        }
        return LabelId.invalid();
    }

    /**
     * Gets and increments the current available type id.
     *
     * @return the current available Grakn id which can be used for types
     */
    private LabelId getNextId() {
        TypeImpl<?, ?> metaConcept = (TypeImpl<?, ?>) getMetaConcept();
        Integer currentValue = metaConcept.vertex().property(Schema.VertexProperty.CURRENT_LABEL_ID);
        if (currentValue == null) {
            currentValue = Schema.MetaSchema.values().length + 1;
        } else {
            currentValue = currentValue + 1;
        }
        //Vertex is used directly here to bypass meta type mutation check
        metaConcept.property(Schema.VertexProperty.CURRENT_LABEL_ID, currentValue);
        return LabelId.of(currentValue);
    }

    /**
     * Gets the config option which determines the number of instances a grakn.core.concept.type.Type must have before the grakn.core.concept.type.Type
     * if automatically sharded.
     *
     * @return the number of instances a grakn.core.concept.type.Type must have before it is shareded
     */
    public long shardingThreshold() {
        return session().config().getProperty(ConfigKey.SHARDING_THRESHOLD);
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
     * @param <T>    The type of the concept being built
     * @param vertex A vertex which contains properties necessary to build a concept from.
     * @return A concept built using the provided vertex
     */
    public <T extends Concept> T buildConcept(Vertex vertex) {
        return factory().buildConcept(vertex);
    }

    /**
     * @param <T>  The type of the Concept being built
     * @param edge An Edge which contains properties necessary to build a Concept from.
     * @return A Concept built using the provided Edge
     */
    public <T extends Concept> T buildConcept(Edge edge) {
        return factory().buildConcept(edge);
    }

    /**
     * Utility function to get a read-only Tinkerpop traversal.
     *
     * @return A read-only Tinkerpop traversal for manually traversing the graph
     */
    public GraphTraversalSource getTinkerTraversal() {
        checkGraphIsOpen();
        if (graphTraversalSource == null) {
            graphTraversalSource = janusTransaction.traversal().withStrategies(ReadOnlyStrategy.instance());
        }
        return graphTraversalSource;
    }

    public ElementFactory factory() {
        return elementFactory;
    }

    /**
     * @param key   The concept property tp search by.
     * @param value The value of the concept
     * @return A concept with the matching key and value
     */
    //----------------------------------------------General Functionality-----------------------------------------------
    public <T extends Concept> T getConcept(Schema.VertexProperty key, Object value) {
        return getConcept(getTinkerTraversal().V().has(key.name(), value));
    }

    private <T extends Concept> T getConcept(Iterator<Vertex> vertices) {
        T concept = null;
        if (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            concept = factory().buildConcept(vertex);
        }
        return concept;
    }

    @Override
    public final Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        Set<SchemaConcept> superSet = new HashSet<>();

        while (schemaConcept != null) {
            superSet.add(schemaConcept);
            schemaConcept = schemaConcept.sup();
        }

        return superSet.stream();
    }

    private Set<Concept> getConcepts(Schema.VertexProperty key, Object value) {
        Set<Concept> concepts = new HashSet<>();
        getTinkerTraversal().V().has(key.name(), value).forEachRemaining(v -> concepts.add(factory().buildConcept(v)));
        return concepts;
    }

    public void checkMutationAllowed() {
        if (Type.READ.equals(type())) throw TransactionException.transactionReadOnly(this);
    }

    /**
     * Adds a new type vertex which occupies a grakn id. This result in the grakn id count on the meta concept to be
     * incremented.
     *
     * @param label    The label of the new type vertex
     * @param baseType The base type of the new type
     * @return The new type vertex
     */
    VertexElement addTypeVertex(LabelId id, Label label, Schema.BaseType baseType) {
        VertexElement vertexElement = addVertexElement(baseType);
        vertexElement.property(Schema.VertexProperty.SCHEMA_LABEL, label.getValue());
        vertexElement.property(Schema.VertexProperty.LABEL_ID, id.getValue());
        return vertexElement;
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

    /**
     * @param label A unique label for the EntityType
     * @return A new or existing EntityType with the provided label
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-EntityType.
     */
    @Override
    public EntityType putEntityType(Label label) {
        return putSchemaConcept(label, Schema.BaseType.ENTITY_TYPE, false,
                v -> factory().buildEntityType(v, getMetaEntityType()));
    }

    /**
     * This is a helper method which will either find or create a SchemaConcept.
     * When a new SchemaConcept is created it is added for validation through it's own creation method for
     * example RoleImpl#create(VertexElement, Role).
     * <p>
     * When an existing SchemaConcept is found it is build via it's get method such as
     * RoleImpl#get(VertexElement) and skips validation.
     * <p>
     * Once the SchemaConcept is found or created a few checks for uniqueness and correct
     * Schema.BaseType are performed.
     *
     * @param label             The Label of the SchemaConcept to find or create
     * @param baseType          The Schema.BaseType of the SchemaConcept to find or create
     * @param isImplicit        a flag indicating if the label we are creating is for an implicit grakn.core.concept.type.Type or not
     * @param newConceptFactory the factory to be using when creating a new SchemaConcept
     * @param <T>               The type of SchemaConcept to return
     * @return a new or existing SchemaConcept
     */
    private <T extends SchemaConcept> T putSchemaConcept(Label label, Schema.BaseType baseType, boolean isImplicit, Function<VertexElement, T> newConceptFactory) {
        //Get the type if it already exists otherwise build a new one
        SchemaConceptImpl schemaConcept = getSchemaConcept(convertToId(label));
        if (schemaConcept == null) {
            if (!isImplicit && label.getValue().startsWith(Schema.ImplicitType.RESERVED.getValue())) {
                throw TransactionException.invalidLabelStart(label);
            }

            VertexElement vertexElement = addTypeVertex(getNextId(), label, baseType);

            //Mark it as implicit here so we don't have to pass it down the constructors
            if (isImplicit) {
                vertexElement.property(Schema.VertexProperty.IS_IMPLICIT, true);
            }

            // if the schema concept is not in janus, create it here
            schemaConcept = SchemaConceptImpl.from(newConceptFactory.apply(vertexElement));
        } else if (!baseType.equals(schemaConcept.baseType())) {
            throw labelTaken(schemaConcept);
        }

        //noinspection unchecked
        return (T) schemaConcept;
    }

    /**
     * Throws an exception when adding a SchemaConcept using a Label which is already taken
     */
    private TransactionException labelTaken(SchemaConcept schemaConcept) {
        if (Schema.MetaSchema.isMetaLabel(schemaConcept.label())) {
            return TransactionException.reservedLabel(schemaConcept.label());
        }
        return PropertyNotUniqueException.cannotCreateProperty(schemaConcept, Schema.VertexProperty.SCHEMA_LABEL, schemaConcept.label());
    }

    private <T extends Concept> T validateSchemaConcept(Concept concept, Schema.BaseType baseType, Supplier<T> invalidHandler) {
        if (concept != null && baseType.getClassType().isInstance(concept)) {
            //noinspection unchecked
            return (T) concept;
        } else {
            return invalidHandler.get();
        }
    }

    /**
     * @param label A unique label for the RelationType
     * @return A new or existing RelationType with the provided label.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-RelationType.
     */
    @Override
    public RelationType putRelationType(Label label) {
        return putSchemaConcept(label, Schema.BaseType.RELATION_TYPE, false,
                v -> factory().buildRelationType(v, getMetaRelationType()));
    }

    public RelationType putRelationTypeImplicit(Label label) {
        return putSchemaConcept(label, Schema.BaseType.RELATION_TYPE, true,
                v -> factory().buildRelationType(v, getMetaRelationType()));
    }

    /**
     * @param label A unique label for the Role
     * @return new or existing Role with the provided Id.
     * @throws TransactionException       if the graph is closed
     * @throws PropertyNotUniqueException if the {@param label} is already in use by an existing non-Role.
     */
    @Override
    public Role putRole(Label label) {
        return putSchemaConcept(label, Schema.BaseType.ROLE, false,
                v -> factory().buildRole(v, getMetaRole()));
    }

    public Role putRoleTypeImplicit(Label label) {
        return putSchemaConcept(label, Schema.BaseType.ROLE, true,
                v -> factory().buildRole(v, getMetaRole()));
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
        @SuppressWarnings("unchecked")
        AttributeType<V> attributeType = putSchemaConcept(label, Schema.BaseType.ATTRIBUTE_TYPE, false,
                v -> factory().buildAttributeType(v, getMetaAttributeType(), dataType));

        //These checks is needed here because caching will return a type by label without checking the datatype
        if (Schema.MetaSchema.isMetaLabel(label)) {
            throw TransactionException.metaTypeImmutable(label);
        } else if (!dataType.equals(attributeType.dataType())) {
            throw TransactionException.immutableProperty(attributeType.dataType(), dataType, Schema.VertexProperty.DATA_TYPE);
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
        Rule rule = putSchemaConcept(label, Schema.BaseType.RULE, false,
                v -> factory().buildRule(v, getMetaRule(), when, then));
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

        // fetch concept from cache if it's cached
        if (transactionCache.isConceptCached(id)) {
            return transactionCache.getCachedConcept(id);
        }

        // If edgeId, we are trying to fetch either:
        // - a concept edge
        // - a reified relation
        if (Schema.isEdgeId(id)) {
            T concept = getConceptEdge(id);
            if (concept != null) return concept;
            // If concept is still null,  it is possible we are referring to a ReifiedRelation which
            // uses its previous EdgeRelation as an id so property must be fetched
            return this.getConcept(Schema.VertexProperty.EDGE_RELATION_ID, id.getValue());
        }

        return getConcept(getTinkerTraversal().V(Schema.elementId(id)));
    }

    @Nullable
    private <T extends Concept> T getConceptEdge(ConceptId id) {
        String edgeId = Schema.elementId(id);
        GraphTraversal<Edge, Edge> traversal = getTinkerTraversal().E(edgeId);
        if (traversal.hasNext()) {
            return factory().buildConcept(factory().buildEdgeElement(traversal.next()));
        }
        return null;
    }

    private <T extends SchemaConcept> T getSchemaConcept(Label label, Schema.BaseType baseType) {
        checkGraphIsOpen();
        SchemaConcept schemaConcept;
        if (transactionCache.isTypeCached(label)) {
            schemaConcept = transactionCache.getCachedSchemaConcept(label);
        } else {
            schemaConcept = getSchemaConcept(convertToId(label));
        }
        return validateSchemaConcept(schemaConcept, baseType, () -> null);
    }

    @Nullable
    public <T extends SchemaConcept> T getSchemaConcept(LabelId id) {
        if (!id.isValid()) return null;
        return this.getConcept(Schema.VertexProperty.LABEL_ID, id.getValue());
    }

    /**
     * @param value A value which an Attribute in the graph may be holding.
     * @param <V>
     * @return The Attributes holding the provided value or an empty collection if no such Attribute exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        if (value == null) return Collections.emptySet();

        // TODO: Remove this forced casting once we replace DataType to be Parameterised Generic Enum
        AttributeType.DataType<V> dataType =
                (AttributeType.DataType<V>) AttributeType.DataType.of(value.getClass());
        if (dataType == null) {
            throw TransactionException.unsupportedDataType(value);
        }

        HashSet<Attribute<V>> attributes = new HashSet<>();
        getConcepts(Schema.VertexProperty.ofDataType(dataType), Serialiser.of(dataType).serialise(value))
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
        Schema.MetaSchema meta = Schema.MetaSchema.valueOf(label);
        if (meta != null) return getSchemaConcept(meta.getId());
        return getSchemaConcept(label, Schema.BaseType.SCHEMA_CONCEPT);
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
        return getSchemaConcept(label, Schema.BaseType.TYPE);
    }

    /**
     * @param label A unique label which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided label or null if no such Entity Type exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ENTITY_TYPE);
    }

    /**
     * @param label A unique label which identifies the RelationType in the graph.
     * @return The RelationType with the provided label or null if no such RelationType exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public RelationType getRelationType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.RELATION_TYPE);
    }

    /**
     * @param label A unique label which identifies the AttributeType in the graph.
     * @param <V>
     * @return The AttributeType with the provided label or null if no such AttributeType exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ATTRIBUTE_TYPE);
    }

    /**
     * @param label A unique label which identifies the Role Type in the graph.
     * @return The Role Type  with the provided label or null if no such Role Type exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ROLE);
    }

    /**
     * @param label A unique label which identifies the Rule in the graph.
     * @return The Rule with the provided label or null if no such Rule Type exists.
     * @throws TransactionException if the graph is closed
     */
    @Override
    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.RULE);
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
            synchronized (keyspaceCache) {
                commitInternal();
                transactionCache.flushToKeyspaceCache();
            }

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
    @VisibleForTesting
    public void shard(ConceptId conceptId) {
        ConceptImpl type = getConcept(conceptId);
        if (type == null) {
            throw new RuntimeException("Cannot shard concept [" + conceptId + "] due to it not existing in the graph");
        }
        type.createShard();
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
        return new QueryExecutor(this, true);
    }

    public final QueryExecutor executor(boolean infer) {
        return new QueryExecutor(this, infer);
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
}