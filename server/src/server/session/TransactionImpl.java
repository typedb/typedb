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

import com.google.auto.value.AutoValue;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.Pattern;
import grakn.core.graql.QueryBuilder;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.LabelId;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.executor.QueryExecutorImpl;
import grakn.core.graql.query.QueryBuilderImpl;
import grakn.core.server.QueryExecutor;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.exception.PropertyNotUniqueException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Validator;
import grakn.core.server.kb.concept.ConceptImpl;
import grakn.core.server.kb.concept.ElementFactory;
import grakn.core.server.kb.concept.RoleImpl;
import grakn.core.server.kb.concept.SchemaConceptImpl;
import grakn.core.server.kb.concept.TypeImpl;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.session.cache.GlobalCache;
import grakn.core.server.session.cache.RuleCache;
import grakn.core.server.session.cache.TransactionCache;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * The {@link Transaction} Base Implementation.
 * This defines how a grakn graph sits on top of a Tinkerpop {@link Graph}.
 * It mostly act as a construction object which ensure the resulting graph conforms to the Grakn Object model.
 *
 * @param <G> A vendor specific implementation of a Tinkerpop {@link Graph}.
 */
public abstract class TransactionImpl<G extends Graph> implements Transaction {
    final Logger LOG = LoggerFactory.getLogger(TransactionImpl.class);

    //----------------------------- Shared Variables
    private final SessionImpl session;
    private final G graph;
    private final ElementFactory elementFactory;
    private final GlobalCache globalCache;


    //----------------------------- Transaction Specific
    private final ThreadLocal<TransactionCache> localConceptLog = new ThreadLocal<>();
    private @Nullable GraphTraversalSource graphTraversalSource = null;
    private final RuleCache ruleCache;

    public TransactionImpl(SessionImpl session, G graph) {
        this.session = session;
        this.graph = graph;
        this.elementFactory = new ElementFactory(this);
        this.ruleCache = new RuleCache(this);

        //Initialise Graph Caches
        globalCache = new GlobalCache(session.config());

        //Initialise Graph
        txCache().openTx(Type.WRITE);

        if (initialiseMetaConcepts()) commit();
    }

    @Override
    public SessionImpl session() {
        return session;
    }

    public RuleCache ruleCache(){ return ruleCache;}

    /**
     * Converts a Type Label into a type Id for this specific graph. Mapping labels to ids will differ between graphs
     * so be sure to use the correct graph when performing the mapping.
     *
     * @param label The label to be converted to the id
     * @return The matching type id
     */
    public LabelId convertToId(Label label) {
        if (txCache().isLabelCached(label)) {
            return txCache().convertLabelToId(label);
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
     * @return The graph cache which contains all the data cached and accessible by all transactions.
     */
    public GlobalCache getGlobalCache() {
        return globalCache;
    }


    /**
     * Opens the thread bound transaction
     */
    public void openTransaction(Type txType) {
        txCache().openTx(txType);
    }

    /**
     * Gets the config option which determines the number of instances a {@link grakn.core.graql.concept.Type} must have before the {@link grakn.core.graql.concept.Type}
     * if automatically sharded.
     *
     * @return the number of instances a {@link grakn.core.graql.concept.Type} must have before it is shareded
     */
    public long shardingThreshold() {
        return session().config().getProperty(ConfigKey.SHARDING_THRESHOLD);
    }

    public TransactionCache txCache() {
        TransactionCache transactionCache = localConceptLog.get();
        if (transactionCache == null) {
            localConceptLog.set(transactionCache = new TransactionCache(getGlobalCache()));
        }

        if (transactionCache.isTxOpen() && transactionCache.schemaNotCached()) {
            transactionCache.refreshSchemaCache();
        }

        return transactionCache;
    }

    @Override
    public boolean isClosed() {
        return !txCache().isTxOpen();
    }

    public abstract boolean isTinkerPopGraphClosed();

    @Override
    public Type txType() {
        return txCache().txType();
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
     * @param <T>  The type of the {@link Concept} being built
     * @param edge An {@link Edge} which contains properties necessary to build a {@link Concept} from.
     * @return A {@link Concept} built using the provided {@link Edge}
     */
    public <T extends Concept> T buildConcept(Edge edge) {
        return factory().buildConcept(edge);
    }

    /**
     * A flag to check if batch loading is enabled and consistency checks are switched off
     *
     * @return true if batch loading is enabled
     */
    public boolean isBatchTx() {
        return Type.BATCH.equals(txCache().txType());
    }

    @SuppressWarnings("unchecked")
    private boolean initialiseMetaConcepts() {
        boolean schemaInitialised = false;
        if (isMetaSchemaNotInitialised()) {
            VertexElement type = addTypeVertex(Schema.MetaSchema.THING.getId(), Schema.MetaSchema.THING.getLabel(), Schema.BaseType.TYPE);
            VertexElement entityType = addTypeVertex(Schema.MetaSchema.ENTITY.getId(), Schema.MetaSchema.ENTITY.getLabel(), Schema.BaseType.ENTITY_TYPE);
            VertexElement relationType = addTypeVertex(Schema.MetaSchema.RELATIONSHIP.getId(), Schema.MetaSchema.RELATIONSHIP.getLabel(), Schema.BaseType.RELATIONSHIP_TYPE);
            VertexElement resourceType = addTypeVertex(Schema.MetaSchema.ATTRIBUTE.getId(), Schema.MetaSchema.ATTRIBUTE.getLabel(), Schema.BaseType.ATTRIBUTE_TYPE);
            addTypeVertex(Schema.MetaSchema.ROLE.getId(), Schema.MetaSchema.ROLE.getLabel(), Schema.BaseType.ROLE);
            addTypeVertex(Schema.MetaSchema.RULE.getId(), Schema.MetaSchema.RULE.getLabel(), Schema.BaseType.RULE);

            relationType.property(Schema.VertexProperty.IS_ABSTRACT, true);
            resourceType.property(Schema.VertexProperty.IS_ABSTRACT, true);
            entityType.property(Schema.VertexProperty.IS_ABSTRACT, true);

            relationType.addEdge(type, Schema.EdgeLabel.SUB);
            resourceType.addEdge(type, Schema.EdgeLabel.SUB);
            entityType.addEdge(type, Schema.EdgeLabel.SUB);

            schemaInitialised = true;
        }

        //Copy entire schema to the graph cache. This may be a bad idea as it will slow down graph initialisation
        copyToCache(getMetaConcept());

        //Role and rule have to be copied separately due to not being connected to meta schema
        copyToCache(getMetaRole());
        copyToCache(getMetaRule());

        return schemaInitialised;
    }


    /**
     * Copies the {@link SchemaConcept} and it's subs into the {@link TransactionCache}.
     * This is important as lookups for {@link SchemaConcept}s based on {@link Label} depend on this caching.
     *
     * @param schemaConcept the {@link SchemaConcept} to be copied into the {@link TransactionCache}
     */
    private void copyToCache(SchemaConcept schemaConcept) {
        schemaConcept.subs().forEach(concept -> {
            getGlobalCache().cacheLabel(concept.label(), concept.labelId());
            getGlobalCache().cacheType(concept.label(), concept);
        });
    }

    private boolean isMetaSchemaNotInitialised() {
        return getMetaConcept() == null;
    }

    public G getTinkerPopGraph() {
        return graph;
    }

    /**
     * Utility function to get a read-only Tinkerpop traversal.
     *
     * @return A read-only Tinkerpop traversal for manually traversing the graph
     */
    public GraphTraversalSource getTinkerTraversal() {
        operateOnOpenGraph(() -> null); //This is to check if the graph is open
        if (graphTraversalSource == null) {
            graphTraversalSource = getTinkerPopGraph().traversal().withStrategies(ReadOnlyStrategy.instance());
        }
        return graphTraversalSource;
    }

    @Override
    public QueryBuilder graql() {
        return new QueryBuilderImpl(this);
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

    public void checkSchemaMutationAllowed() {
        checkMutationAllowed();
        if (isBatchTx()) throw TransactionException.schemaMutation();
    }

    public void checkMutationAllowed() {
        if (Type.READ.equals(txType())) throw TransactionException.transactionReadOnly(this);
    }


    public VertexElement addVertexElement(Schema.BaseType baseType, ConceptId... conceptIds) {
        return factory().addVertexElement(baseType, conceptIds);
    }

    /**
     * Adds a new type vertex which occupies a grakn id. This result in the grakn id count on the meta concept to be
     * incremented.
     *
     * @param label    The label of the new type vertex
     * @param baseType The base type of the new type
     * @return The new type vertex
     */
    private VertexElement addTypeVertex(LabelId id, Label label, Schema.BaseType baseType) {
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
        if (isClosed()) throw TransactionException.transactionClosed(this, txCache().getClosedReason());
        return supplier.get();
    }

    @Override
    public EntityType putEntityType(Label label) {
        return putSchemaConcept(label, Schema.BaseType.ENTITY_TYPE, false,
                v -> factory().buildEntityType(v, getMetaEntityType()));
    }

    /**
     * This is a helper method which will either find or create a {@link SchemaConcept}.
     * When a new {@link SchemaConcept} is created it is added for validation through it's own creation method for
     * example {@link RoleImpl#create(VertexElement, Role)}.
     * <p>
     * When an existing {@link SchemaConcept} is found it is build via it's get method such as
     * {@link RoleImpl#get(VertexElement)} and skips validation.
     * <p>
     * Once the {@link SchemaConcept} is found or created a few checks for uniqueness and correct
     * {@link Schema.BaseType} are performed.
     *
     * @param label             The {@link Label} of the {@link SchemaConcept} to find or create
     * @param baseType          The {@link Schema.BaseType} of the {@link SchemaConcept} to find or create
     * @param isImplicit        a flag indicating if the label we are creating is for an implicit {@link grakn.core.graql.concept.Type} or not
     * @param newConceptFactory the factory to be using when creating a new {@link SchemaConcept}
     * @param <T>               The type of {@link SchemaConcept} to return
     * @return a new or existing {@link SchemaConcept}
     */
    private <T extends SchemaConcept> T putSchemaConcept(Label label, Schema.BaseType baseType, boolean isImplicit, Function<VertexElement, T> newConceptFactory) {
        checkSchemaMutationAllowed();

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

            schemaConcept = SchemaConceptImpl.from(buildSchemaConcept(label, () -> newConceptFactory.apply(vertexElement)));
        } else if (!baseType.equals(schemaConcept.baseType())) {
            throw labelTaken(schemaConcept);
        }

        //noinspection unchecked
        return (T) schemaConcept;
    }

    /**
     * Throws an exception when adding a {@link SchemaConcept} using a {@link Label} which is already taken
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
     * A helper method which either retrieves the {@link SchemaConcept} from the cache or builds it using a provided supplier
     *
     * @param label     The {@link Label} of the {@link SchemaConcept} to retrieve or build
     * @param dbBuilder A method which builds the {@link SchemaConcept} via a DB read or write
     * @return The {@link SchemaConcept} which was either cached or built via a DB read or write
     */
    private SchemaConcept buildSchemaConcept(Label label, Supplier<SchemaConcept> dbBuilder) {
        if (txCache().isTypeCached(label)) {
            return txCache().getCachedSchemaConcept(label);
        } else {
            return dbBuilder.get();
        }
    }

    @Override
    public RelationshipType putRelationshipType(Label label) {
        return putSchemaConcept(label, Schema.BaseType.RELATIONSHIP_TYPE, false,
                v -> factory().buildRelationshipType(v, getMetaRelationType()));
    }

    public RelationshipType putRelationTypeImplicit(Label label) {
        return putSchemaConcept(label, Schema.BaseType.RELATIONSHIP_TYPE, true,
                v -> factory().buildRelationshipType(v, getMetaRelationType()));
    }

    @Override
    public Role putRole(Label label) {
        return putSchemaConcept(label, Schema.BaseType.ROLE, false,
                v -> factory().buildRole(v, getMetaRole()));
    }

    public Role putRoleTypeImplicit(Label label) {
        return putSchemaConcept(label, Schema.BaseType.ROLE, true,
                v -> factory().buildRole(v, getMetaRole()));
    }

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

    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        Rule rule = putSchemaConcept(label, Schema.BaseType.RULE, false,
                v -> factory().buildRule(v, getMetaRule(), when, then));
        //NB: thenTypes() will be empty as type edges added on commit
        //NB: this will cache also non-committed rules
        if (rule.then() != null){
            rule.then().admin().varPatterns().stream()
                    .flatMap(v -> v.getTypeLabels().stream())
                    .map(vl -> this.<SchemaConcept>getSchemaConcept(vl))
                    .filter(Objects::nonNull)
                    .filter(Concept::isType)
                    .forEach(type -> ruleCache.updateRules(type, rule));
        }
        return rule;
    }

    //------------------------------------ Lookup
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        return operateOnOpenGraph(() -> {
            if (txCache().isConceptCached(id)) {
                return txCache().getCachedConcept(id);
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

        SchemaConcept schemaConcept = buildSchemaConcept(label, () -> getSchemaConcept(convertToId(label)));
        return validateSchemaConcept(schemaConcept, baseType, () -> null);
    }

    @Nullable
    public <T extends SchemaConcept> T getSchemaConcept(LabelId id) {
        if (!id.isValid()) return null;
        return this.<T>getConcept(Schema.VertexProperty.LABEL_ID, id.getValue()).orElse(null);
    }

    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        if (value == null) return Collections.emptySet();

        //Make sure you trying to retrieve supported data type
        if (!AttributeType.DataType.SUPPORTED_TYPES.containsKey(value.getClass().getName())) {
            throw TransactionException.unsupportedDataType(value);
        }

        HashSet<Attribute<V>> attributes = new HashSet<>();
        AttributeType.DataType dataType = AttributeType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName());

        //noinspection unchecked
        getConcepts(dataType.getVertexProperty(), dataType.getPersistenceValue(value)).forEach(concept -> {
            if (concept != null && concept.isAttribute()) {
                //noinspection unchecked
                attributes.add(concept.asAttribute());
            }
        });

        return attributes;
    }

    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        Schema.MetaSchema meta = Schema.MetaSchema.valueOf(label);
        if (meta != null) return getSchemaConcept(meta.getId());
        return getSchemaConcept(label, Schema.BaseType.SCHEMA_CONCEPT);
    }

    @Override
    public <T extends grakn.core.graql.concept.Type> T getType(Label label) {
        return getSchemaConcept(label, Schema.BaseType.TYPE);
    }

    @Override
    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ENTITY_TYPE);
    }

    @Override
    public RelationshipType getRelationshipType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.RELATIONSHIP_TYPE);
    }

    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ATTRIBUTE_TYPE);
    }

    @Override
    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ROLE);
    }

    @Override
    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.RULE);
    }

    //This is overridden by vendors for more efficient clearing approaches
    public void clearGraph() {
        getTinkerPopGraph().traversal().V().drop().iterate();
    }

    /**
     * Closes the root session this graph stems from. This will automatically rollback any pending transactions.
     */
    public void closeSession() {
        try {
            txCache().closeTx(ErrorMessage.SESSION_CLOSED.getMessage(keyspace()));
            getTinkerPopGraph().close();
        } catch (Exception e) {
            throw TransactionException.closingFailed(this, e);
        }
    }

    /**
     * Close the transaction without committing
     */
    @Override
    public void close() {
        if (isClosed()) {
            return;
        }
        try {
            txCache().writeToGraphCache(txType().equals(Type.READ));
        } finally {
            String closeMessage = ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("closed", keyspace());
            closeTransaction(closeMessage);
        }
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
        String closeMessage = ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("committed", keyspace());
        try {
            validateGraph();
            commitTransactionInternal();
            txCache().writeToGraphCache(true);
        } finally {
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
            graph.tx().close();
        } catch (UnsupportedOperationException e) {
            //Ignored for Tinker
        } finally {
            txCache().closeTx(closedReason);
            ruleCache().closeTx();
        }
    }


    private Optional<CommitLog> commitWithLogs() throws InvalidKBException {
        validateGraph();

        Map<ConceptId, Long> newInstances = txCache().getShardingCount();
        Map<String, Set<ConceptId>> newAttributes = txCache().getNewAttributes();
        boolean logsExist = !newInstances.isEmpty() || !newAttributes.isEmpty();

        commitTransactionInternal();

        txCache().writeToGraphCache(true);

        //If we have logs to commit get them and add them
        if (logsExist) {
            return Optional.of(CommitLog.create(keyspace(), newInstances, newAttributes));
        }


        return Optional.empty();
    }

    public void commitTransactionInternal() {
        try {
            LOG.trace("Graph is valid. Committing graph . . . ");
            getTinkerPopGraph().tx().commit();
            LOG.trace("Graph committed.");
        } catch (UnsupportedOperationException e) {
            //IGNORED
        }
    }

    private void validateGraph() throws InvalidKBException {
        Validator validator = new Validator(this);
        if (!validator.validate()) {
            List<String> errors = validator.getErrorsFound();
            if (!errors.isEmpty()) throw InvalidKBException.validationErrors(errors);
        }
    }


    public boolean isValidElement(Element element) {
        return element != null;
    }

    //------------------------------------------ Fixing Code for Postprocessing ----------------------------------------

    /**
     * Returns the duplicates of the given concept
     *
     * @param mainConcept primary concept - this one is returned by the index and not considered a duplicate
     * @param conceptIds  Set of Ids containing potential duplicates of the main concept
     * @return a set containing the duplicates of the given concept
     */
    private <X extends ConceptImpl> Set<X> getDuplicates(X mainConcept, Set<ConceptId> conceptIds) {
        Set<X> duplicated = conceptIds.stream()
                .map(this::<X>getConcept)
                //filter non-null, will be null if previously deleted/merged
                .filter(Objects::nonNull)
                .collect(toSet());

        duplicated.remove(mainConcept);

        return duplicated;
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
     * Returns the current number of shards the provided {@link grakn.core.graql.concept.Type} has. This is used in creating more
     * efficient query plans.
     *
     * @param concept The {@link grakn.core.graql.concept.Type} which may contain some shards.
     * @return the number of Shards the {@link grakn.core.graql.concept.Type} currently has.
     */
    public long getShardCount(grakn.core.graql.concept.Type concept) {
        return TypeImpl.from(concept).shardCount();
    }

    @Override
    public final QueryExecutor queryExecutor() {
        return QueryExecutorImpl.create(this);
    }

    /**
     * Stores the commit log of a {@link Transaction}.
     * Stores the commit log of a {@link Transaction} which is uploaded to the jserver when the {@link Session} is closed.
     * The commit log is also uploaded periodically to make sure that if a failure occurs the counts are still roughly maintained.
     */
    @AutoValue
    public abstract static class CommitLog {

        public abstract Keyspace keyspace();

        public abstract Map<ConceptId, Long> instanceCount();

        public abstract Map<String, Set<ConceptId>> attributes();

        public static CommitLog create(Keyspace keyspace, Map<ConceptId, Long> instanceCount, Map<String, Set<ConceptId>> newAttributes){
            return new AutoValue_TransactionImpl_CommitLog(keyspace, instanceCount, newAttributes);
        }
    }
}
