/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

import brave.ScopedSpan;
import grakn.benchmark.lib.serverinstrumentation.ServerTracingInstrumentation;
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
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.graql.executor.QueryExecutor;
import grakn.core.server.exception.GraknServerException;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.PropertyNotUniqueException;
import grakn.core.server.exception.TemporaryWriteException;
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
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A TransactionOLTP using JanusGraph as a vendor backend.
 *
 * Wraps a TinkerPop transaction (the graph is still needed as we use to retrieve tinker traversals)
 *
 * With this vendor some issues to be aware of:
 * 2. Clearing the graph explicitly closes the connection as well.
 */
public class TransactionOLTP implements Transaction {
    final Logger LOG = LoggerFactory.getLogger(TransactionOLTP.class);
    // Shared Variables
    private final SessionImpl session;
    private final JanusGraph janusGraph;
    private final ElementFactory elementFactory;

    // Caches
    private final RuleCache ruleCache;
    private final KeyspaceCache keyspaceCache;
    private final TransactionCache transactionCache;

    // TransactionOLTP Specific
    private final org.apache.tinkerpop.gremlin.structure.Transaction janusTransaction;
    private Transaction.Type txType;
    private String closedReason = null;
    private boolean isTxOpen;

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

    TransactionOLTP(SessionImpl session, JanusGraph janusGraph, KeyspaceCache keyspaceCache) {

        createdInCurrentThread.set(true);

        this.session = session;
        this.janusGraph = janusGraph;

        this.janusTransaction = janusGraph.tx();

        this.elementFactory = new ElementFactory(this, janusGraph);

        this.ruleCache = new RuleCache(this);

        this.keyspaceCache = keyspaceCache;
        this.transactionCache = new TransactionCache(keyspaceCache);

    }

    void open(Type type) {
        this.txType = type;
        this.isTxOpen = true;
        this.janusTransaction.open();
        this.transactionCache.updateSchemaCacheFromKeyspaceCache();
    }


    private void commitTransactionInternal() {
        executeLockingMethod(() -> {
            try {
                LOG.trace("Graph is valid. Committing graph . . . ");
                janusTransaction.commit();
                LOG.trace("Graph committed.");
            } catch (UnsupportedOperationException e) {
                //IGNORED
            }
            return null;
        });
    }

    public VertexElement addVertexElement(Schema.BaseType baseType) {
        return executeLockingMethod(() -> factory().addVertexElement(baseType));
    }

    /**
     * This is only used when reifying a Relation
     * @param baseType Concept BaseType which will become the VertexLabel
     * @param conceptId ConceptId to be set on the vertex
     * @return just created Vertex
     */
    public VertexElement addVertexElement(Schema.BaseType baseType, ConceptId conceptId) {
        return executeLockingMethod(() -> factory().addVertexElement(baseType, conceptId));
    }

    /**
     * Executes a method which has the potential to throw a TemporaryLockingException or a PermanentLockingException.
     * If the exception is thrown it is wrapped in a GraknServerException so that the transaction can be retried.
     *
     * @param method The locking method to execute
     */
    private <X> X executeLockingMethod(Supplier<X> method) {
        try {
            return method.get();
        } catch (JanusGraphException e) {
            if (e.isCausedBy(TemporaryLockingException.class) || e.isCausedBy(PermanentLockingException.class)) {
                throw TemporaryWriteException.temporaryLock(e);
            } else {
                throw GraknServerException.unknown(e);
            }
        }
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
    public Stream<Numeric> stream(GraqlCompute.Statistics query, boolean infer) {
        return executor(infer).compute(query);
    }

    @Override
    public Stream<ConceptList> stream(GraqlCompute.Path query, boolean infer) {
        return executor(infer).compute(query);
    }

    @Override
    public Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query, boolean infer) {
        return executor(infer).compute(query);
    }

    @Override
    public Stream<ConceptSet> stream(GraqlCompute.Cluster query, boolean infer) {
        return executor(infer).compute(query);
    }

    public RuleCache ruleCache() { return ruleCache;}

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
        operateOnOpenGraph(() -> null); //This is to check if the graph is open
        if (graphTraversalSource == null) {
            graphTraversalSource = janusGraph.traversal().withStrategies(ReadOnlyStrategy.instance());
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
    public <T extends Concept> Optional<T> getConcept(Schema.VertexProperty key, Object value) {
        Iterator<Vertex> vertices = getTinkerTraversal().V().has(key.name(), value);

        if (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            return Optional.of(factory().buildConcept(vertex));
        } else {
            return Optional.empty();
        }
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
    protected VertexElement addTypeVertex(LabelId id, Label label, Schema.BaseType baseType) {
        VertexElement vertexElement = addVertexElement(baseType);
        vertexElement.property(Schema.VertexProperty.SCHEMA_LABEL, label.getValue());
        vertexElement.property(Schema.VertexProperty.LABEL_ID, id.getValue());
        return vertexElement;
    }

    /**
     * An operation on the graph which requires it to be open.
     *
     * @param supplier The operation to be performed on the graph
     * @return The result of the operation on the graph.
     * @throws TransactionException if the graph is closed.
     */
    private <X> X operateOnOpenGraph(Supplier<X> supplier) {
        if (!isLocal() || isClosed()) throw TransactionException.transactionClosed(this, this.closedReason);
        return supplier.get();
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
     *
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
     * @param id A unique identifier for the Concept in the graph.
     * @param <T>
     * @return The Concept with the provided id or null if no such Concept exists.
     * @throws TransactionException if the graph is closed
     * @throws ClassCastException   if the concept is not an instance of T
     */
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        return operateOnOpenGraph(() -> {
            if (transactionCache.isConceptCached(id)) {
                return transactionCache.getCachedConcept(id);
            } else {
                if (id.getValue().startsWith(Schema.PREFIX_EDGE)) {
                    Optional<T> concept = getConceptEdge(id);
                    if (concept.isPresent()) return concept.get();
                }
                return this.<T>getConcept(Schema.VertexProperty.ID, id.getValue()).orElse(null);
            }
        });
    }

    private <T extends Concept> Optional<T> getConceptEdge(ConceptId id) {
        String edgeId = id.getValue().substring(1);
        GraphTraversal<Edge, Edge> traversal = getTinkerTraversal().E(edgeId);
        if (traversal.hasNext()) {
            return Optional.of(factory().buildConcept(factory().buildEdgeElement(traversal.next())));
        }
        return Optional.empty();
    }

    private <T extends SchemaConcept> T getSchemaConcept(Label label, Schema.BaseType baseType) {
        operateOnOpenGraph(() -> null); //Makes sure the graph is open

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
        return this.<T>getConcept(Schema.VertexProperty.LABEL_ID, id.getValue()).orElse(null);
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
        transactionCache.refreshKeyspaceCache();
        closeTransaction(closeMessage);
    }

    /**
     * Commits and closes the transaction without returning CommitLog
     *
     * @throws InvalidKBException
     */
    @Override
    public void commit() throws InvalidKBException {
        if (isClosed()) {
            return;
        }
        try {
            checkMutationAllowed();
            validateGraph();
            // lock on the keyspace cache shared between concurrent tx's to the same keyspace
            // force serialization & atomic updates, keeping Janus and our KeyspaceCache in sync
            synchronized (keyspaceCache) {
                commitTransactionInternal();
                transactionCache.flushToKeyspaceCache();
            }
        } finally {
            String closeMessage = ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("committed", keyspace());
            closeTransaction(closeMessage);
        }
    }

    /**
     * Commits, closes transaction and returns CommitLog.
     *
     * @return the commit log that would have been submitted if it is needed.
     * @throws InvalidKBException when the graph does not conform to the object concept
     */
    public Optional<CommitLog> commitAndGetLogs() throws InvalidKBException {
        if (isClosed()) {
            return Optional.empty();
        }
        try {
            return commitWithLogs();
        } finally {
            String closeMessage = ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("committed", keyspace());
            closeTransaction(closeMessage);
        }
    }

    private void closeTransaction(String closedReason) {
        try {
            janusTransaction.close();
        } catch (UnsupportedOperationException e) {
            //Ignored for Tinker
        } finally {
            transactionCache.closeTx();
            this.closedReason = closedReason;
            this.isTxOpen = false;
            ruleCache().clear();
        }
    }


    private Optional<CommitLog> commitWithLogs() throws InvalidKBException {

        /* This method has permanent tracing because commits can take varying lengths of time depending on operations */
        ScopedSpan span = null;
        if (ServerTracingInstrumentation.tracingActive()) {
            span = ServerTracingInstrumentation.createScopedChildSpan("commitWithLogs validate");
        }

        checkMutationAllowed();
        validateGraph();

        Map<ConceptId, Long> newInstances = transactionCache.getShardingCount();
        Map<String, Set<ConceptId>> newAttributes = transactionCache.getNewAttributes();
        boolean logsExist = !newInstances.isEmpty() || !newAttributes.isEmpty();

        if (span != null) {
            span.finish();
            span = ServerTracingInstrumentation.createScopedChildSpan("commitWithLogs commit");
        }

        // lock on the keyspace cache shared between concurrent tx's to the same keyspace
        // force serialized updates, keeping Janus and our KeyspaceCache in sync
        synchronized (keyspaceCache) {
            commitTransactionInternal();
            transactionCache.flushToKeyspaceCache();
        }

        //If we have logs to commit get them and add them
        if (logsExist) {

            if (span != null) {
                span.finish();
                span = ServerTracingInstrumentation.createScopedChildSpan("commitWithLogs create log");
            }

            Optional logs = Optional.of(CommitLog.create(keyspace(), newInstances, newAttributes));

            if (span != null)  { span.finish(); }

            return logs;
        }


        return Optional.empty();
    }

    private void validateGraph() throws InvalidKBException {
        Validator validator = new Validator(this);
        if (!validator.validate()) {
            List<String> errors = validator.getErrorsFound();
            if (!errors.isEmpty()) throw InvalidKBException.validationErrors(errors);
        }
    }

    /**
     * Creates a new shard for the concept
     *
     * @param conceptId the id of the concept to shard
     */
    public void shard(ConceptId conceptId) {
        ConceptImpl type = getConcept(conceptId);
        if (type == null) {
            LOG.warn("Cannot shard concept [" + conceptId + "] due to it not existing in the graph");
        } else {
            type.createShard();
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

    /**
     * Stores the commit log of a TransactionOLTP which is uploaded to the jserver when the Session is closed.
     * The commit log is also uploaded periodically to make sure that if a failure occurs the counts are still roughly maintained.
     */
    public static class CommitLog {

        private final KeyspaceImpl keyspace;
        private final Map<ConceptId, Long> instanceCount;
        private final Map<String, Set<ConceptId>> attributes;

        CommitLog(KeyspaceImpl keyspace, Map<ConceptId, Long> instanceCount, Map<String, Set<ConceptId>> attributes) {
            if (keyspace == null) {
                throw new NullPointerException("Null keyspace");
            }
            this.keyspace = keyspace;
            if (instanceCount == null) {
                throw new NullPointerException("Null instanceCount");
            }
            this.instanceCount = instanceCount;
            if (attributes == null) {
                throw new NullPointerException("Null attributes");
            }
            this.attributes = attributes;
        }

        public KeyspaceImpl keyspace() {
            return keyspace;
        }

        public Map<ConceptId, Long> instanceCount() {
            return instanceCount;
        }

        public Map<String, Set<ConceptId>> attributes() {
            return attributes;
        }

        public static CommitLog create(KeyspaceImpl keyspace, Map<ConceptId, Long> instanceCount, Map<String, Set<ConceptId>> newAttributes) {
            return new CommitLog(keyspace, instanceCount, newAttributes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TransactionOLTP.CommitLog that = (TransactionOLTP.CommitLog) o;

            return (this.keyspace.equals(that.keyspace()) &&
                    this.instanceCount.equals(that.instanceCount()) &&
                    this.attributes.equals(that.attributes()));
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^= this.keyspace.hashCode();
            h *= 1000003;
            h ^= this.instanceCount.hashCode();
            h *= 1000003;
            h ^= this.attributes.hashCode();
            return h;
        }
    }
}