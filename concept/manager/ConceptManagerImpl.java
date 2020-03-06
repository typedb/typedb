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

package grakn.core.concept.manager;

import grakn.core.concept.impl.AttributeImpl;
import grakn.core.concept.impl.AttributeTypeImpl;
import grakn.core.concept.impl.EntityImpl;
import grakn.core.concept.impl.EntityTypeImpl;
import grakn.core.concept.impl.RelationEdge;
import grakn.core.concept.impl.RelationImpl;
import grakn.core.concept.impl.RelationReified;
import grakn.core.concept.impl.RelationTypeImpl;
import grakn.core.concept.impl.RoleImpl;
import grakn.core.concept.impl.RuleImpl;
import grakn.core.concept.impl.TypeImpl;
import grakn.core.concept.structure.ElementFactory;
import grakn.core.core.AttributeSerialiser;
import grakn.core.core.AttributeValueConverter;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.RelationStructure;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.concept.structure.VertexElement;
import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.server.cache.TransactionCache;
import grakn.core.kb.server.exception.TemporaryWriteException;
import graql.lang.pattern.Pattern;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static grakn.core.core.Schema.BaseType.ATTRIBUTE;
import static grakn.core.core.Schema.BaseType.ATTRIBUTE_TYPE;
import static grakn.core.core.Schema.BaseType.ENTITY;
import static grakn.core.core.Schema.BaseType.ENTITY_TYPE;
import static grakn.core.core.Schema.BaseType.RELATION;
import static grakn.core.core.Schema.BaseType.RELATION_TYPE;
import static grakn.core.core.Schema.BaseType.ROLE;
import static grakn.core.core.Schema.BaseType.RULE;

/**
 * Class handling all creation and retrieval of concepts
 *
 * In general, it will do one of the following primary operations
 * 1. `Create` a brand new Janus Vertex wrapped in a Schema Concept
 * 2. `Create` a brand new Janus Vertex or Edge, wrapped in a Thing Concept
 * 3. `Build` a Concept from an existing VertexElement or EdgeElement that has been provided from externally
 * 4. `Retrieve` a concept based on some unique identifier (eg. ID, attribute key, janus key/value, etc.)
 *
 * Where possible, the TransactionCache will be queried to avoid rebuilding a concept that is already built or retrieved
 */
public class ConceptManagerImpl implements ConceptManager {

    private ElementFactory elementFactory;
    private TransactionCache transactionCache;
    private ConceptNotificationChannel conceptNotificationChannel;
    private final AttributeManager attributeManager;

    public ConceptManagerImpl(ElementFactory elementFactory, TransactionCache transactionCache, ConceptNotificationChannel conceptNotificationChannel, AttributeManager attributeManager) {
        this.elementFactory = elementFactory;
        this.transactionCache = transactionCache;
        this.conceptNotificationChannel = conceptNotificationChannel;
        this.attributeManager = attributeManager;
    }

    /*

        ----- CREATE behaviors for Schema concepts -----

     */

    /**
     * @param label             The Label of the SchemaConcept to create
     * @param baseType          The Schema.BaseType of the SchemaConcept to find or create
     * @param isImplicit        a flag indicating if the label we are creating is for an implicit grakn.core.kb.concept.api.Type or not
     */
    private VertexElement createSchemaVertex(Label label, Schema.BaseType baseType, boolean isImplicit) {
        if (!isImplicit && label.getValue().startsWith(Schema.ImplicitType.RESERVED.getValue())) {
            throw GraknConceptException.invalidLabelStart(label);
        }

        VertexElement vertexElement = addTypeVertex(getNextId(), label, baseType);

        //Mark it as implicit here so we don't have to pass it down the constructors
        if (isImplicit) {
            vertexElement.property(Schema.VertexProperty.IS_IMPLICIT, true);
        }

        return vertexElement;
    }

    public EntityType createEntityType(Label label, EntityType superType) {
        VertexElement vertex = createSchemaVertex(label, ENTITY_TYPE, false);
        EntityTypeImpl entityType = new EntityTypeImpl(vertex, this, conceptNotificationChannel);
        entityType.createShard();
        entityType.sup(superType);
        transactionCache.cacheConcept(entityType);
        return entityType;
    }

    public RelationType createRelationType(Label label, RelationType superType) {
        VertexElement vertex = createSchemaVertex(label, RELATION_TYPE, false);
        return createRelationType(vertex, superType);
    }

    // users cannot create implicit relation types themselves, this is internal behavior
    @Override
    public RelationType createImplicitRelationType(Label implicitRelationLabel) {
        VertexElement vertex = createSchemaVertex(implicitRelationLabel, RELATION_TYPE, true);
        return createRelationType(vertex, getMetaRelationType());
    }


    @Override
    public <V> AttributeType<V> createAttributeType(Label label, AttributeType<V> superType, AttributeType.DataType<V> dataType) {
        VertexElement vertexElement = createSchemaVertex(label, ATTRIBUTE_TYPE, false);
        vertexElement.propertyImmutable(Schema.VertexProperty.DATA_TYPE, dataType, null, AttributeType.DataType::name);
        AttributeType<V> attributeType = new AttributeTypeImpl<>(vertexElement, this, conceptNotificationChannel);
        attributeType.createShard();
        attributeType.sup(superType);
        transactionCache.cacheConcept(attributeType);
        return attributeType;
    }

    public Role createRole(Label label, Role superType) {
        VertexElement vertexElement = createSchemaVertex(label, ROLE, false);
        return createRole(vertexElement, superType);
    }

    // users cannot create implicit roles themselves, this is internal behavior
    @Override
    public Role createImplicitRole(Label implicitRoleLabel) {
        VertexElement vertexElement = createSchemaVertex(implicitRoleLabel, ROLE, true);
        return createRole(vertexElement, getMetaRole());
    }

    public Rule createRule(Label label, Pattern when, Pattern then, Rule superType) {
        VertexElement vertexElement = createSchemaVertex(label, RULE, false);
        vertexElement.propertyImmutable(Schema.VertexProperty.RULE_WHEN, when, null, Pattern::toString);
        vertexElement.propertyImmutable(Schema.VertexProperty.RULE_THEN, then, null, Pattern::toString);
        RuleImpl rule = new RuleImpl(vertexElement, this, conceptNotificationChannel);
        rule.sup(superType);
        conceptNotificationChannel.ruleCreated(rule);
        transactionCache.cacheConcept(rule);
        return rule;
    }


    private RelationTypeImpl createRelationType(VertexElement vertex, RelationType superType) {
        RelationTypeImpl relationType = new RelationTypeImpl(vertex, this, conceptNotificationChannel);
        relationType.createShard();
        relationType.sup(superType);
        conceptNotificationChannel.relationTypeCreated(relationType);
        transactionCache.cacheConcept(relationType);
        return relationType;
    }

    private RoleImpl createRole(VertexElement vertex, Role superType) {
        RoleImpl role = new RoleImpl(vertex, this, conceptNotificationChannel);
        role.sup(superType);
        transactionCache.cacheConcept(role);
        conceptNotificationChannel.roleCreated(role);
        return role;
    }

    /**
     * Adds a new type vertex which occupies a grakn id. This result in the grakn id count on the meta concept to be
     * incremented.
     *
     * @param label    The label of the new type vertex
     * @param baseType The base type of the new type
     * @return The new type vertex
     * <p>
     * TODO this should not be public
     */
    public VertexElement addTypeVertex(LabelId id, Label label, Schema.BaseType baseType) {
        VertexElement vertexElement = elementFactory.addVertexElement(baseType);
        vertexElement.property(Schema.VertexProperty.SCHEMA_LABEL, label.getValue());
        vertexElement.property(Schema.VertexProperty.LABEL_ID, id.getValue());
        return vertexElement;
    }


    /**
     * Gets and increments the current available type id.
     *
     * TODO this is probably not designed to handle concurrency and distributed nodes - don't do concurrent schema writes for now
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


    /*

        ------- CREATE behaviors for Thing instances ------

     */

    /**
     * Create a new attribute instance from a vertex, skip checking caches because this should be a brand new vertex
     * @param type - the Concept type
     * @param value - value saved in the attribute
     * @param isInferred - if the new concept is inferred or concrete
     * @param <V> - attribute type
     * @return - new Attribute Concept
     */
    @Override
    public <V> AttributeImpl<V> createAttribute(AttributeType<V> type, V value, boolean isInferred) {
        preCheckForInstanceCreation(type);

        VertexElement vertex = createInstanceVertex(ATTRIBUTE, isInferred);

        AttributeType.DataType<V> dataType = type.dataType();

        V convertedValue;
        try {
            convertedValue = AttributeValueConverter.of(type.dataType()).convert(value);
        } catch (ClassCastException e){
            throw GraknConceptException.invalidAttributeValue(type, value, dataType);
        }

        // set persisted value
        Object valueToPersist = AttributeSerialiser.of(dataType).serialise(convertedValue);
        Schema.VertexProperty property = Schema.VertexProperty.ofDataType(dataType);
        vertex.propertyImmutable(property, valueToPersist, null);

        // set unique index - combination of type and value to an indexed Janus property, used for lookups
        String index = Schema.generateAttributeIndex(type.label(), convertedValue.toString());
        vertex.property(Schema.VertexProperty.INDEX, index);

        AttributeImpl<V> newAttribute = new AttributeImpl<>(vertex, this, conceptNotificationChannel);
        newAttribute.type(TypeImpl.from(type));

        conceptNotificationChannel.attributeCreated(newAttribute, value, isInferred);
        return newAttribute;
    }

    /**
     * Create a new Relation instance from an edge
     * Skip checking caches because this should be a brand new edge and concept
     */
    @Override
    public RelationImpl createHasAttributeRelation(EdgeElement edge, RelationType relationType, Role owner, Role value, boolean isInferred) {
        preCheckForInstanceCreation(relationType);

        edge.propertyImmutable(Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID, owner, null, o -> o.labelId().getValue());
        edge.propertyImmutable(Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID, value, null, v -> v.labelId().getValue());
        edge.propertyImmutable(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID, relationType, null, t -> t.labelId().getValue());

        RelationEdge relationEdge = new RelationEdge(edge, this, conceptNotificationChannel);
        // because the Relation hierarchy is still wrong, RelationEdge and RelationImpl doesn't set type(type) like other instances do
        RelationImpl newRelation = new RelationImpl(relationEdge);
        conceptNotificationChannel.hasAttributeRelationCreated(newRelation, isInferred);

        return newRelation;
    }

    /**
     * Used by RelationEdge when it needs to reify a relation
     * NOTE The passed in vertex is already prepared outside of the ConceptManager
     * @return ReifiedRelation
     */
    @Override
    public RelationStructure createRelationReified(VertexElement vertex, RelationType type) {
        preCheckForInstanceCreation(type);
        RelationReified relationReified = new RelationReified(vertex, this, conceptNotificationChannel);
        relationReified.type(TypeImpl.from(type));
        return relationReified;
    }

    /**
     * Create a new Relation instance from a vertex
     * Skip checking caches because this should be a brand new vertex and concept
     */
    @Override
    public RelationImpl createRelation(RelationType type, boolean isInferred) {
        preCheckForInstanceCreation(type);
        VertexElement vertex = createInstanceVertex(RELATION, isInferred);

        // safe downcast from interface to implementation type
        RelationReified relationReified = (RelationReified) createRelationReified(vertex, type);

        RelationImpl newRelation = new RelationImpl(relationReified);
        conceptNotificationChannel.relationCreated(newRelation, isInferred);

        return newRelation;
    }

    /**
     * Create a new Entity instance from a vertex
     * Skip checking caches because this should be a brand new vertex and concept
     */
    @Override
    public EntityImpl createEntity(EntityType type, boolean isInferred) {
        preCheckForInstanceCreation(type);
        VertexElement vertex = createInstanceVertex(ENTITY, isInferred);
        EntityImpl newEntity = new EntityImpl(vertex, this, conceptNotificationChannel);
        newEntity.type(TypeImpl.from(type));

        conceptNotificationChannel.entityCreated(newEntity, isInferred);

        return newEntity;
    }


    private VertexElement createInstanceVertex(Schema.BaseType baseType, boolean isInferred) {
        VertexElement vertexElement = elementFactory.addVertexElement(baseType);
        if (isInferred) {
            vertexElement.property(Schema.VertexProperty.IS_INFERRED, true);
        }
        return vertexElement;
    }

    /**
     * Checks if an Thing is allowed to be created and linked to this Type.
     * It can also fail when attempting to attach an Attribute to a meta type
     */
    private void preCheckForInstanceCreation(Type type) {
        if (Schema.MetaSchema.isMetaLabel(type.label())) {
            throw GraknConceptException.metaTypeImmutable(type.label());
        }
        if (type.isAbstract()) {
            throw GraknConceptException.addingInstancesToAbstractType(type);
        }
    }




    /*

        ---------- RETRIEVE behaviors --------

     */

    /**
     * Check the transaction cache to see if we have the attribute already by index
     * return NULL if attribtue does not exist in cache
     */
    @Override
    @Nullable
    public Attribute getCachedAttribute(String index) {
        return transactionCache.getAttributeCache().get(index);
    }

    /**
     * This is only used when checking if attribute exists before trying to create a new one.
     */
    @Override
    public Attribute getAttribute(String index) {
        Attribute concept = getCachedAttribute(index);
        if (concept != null) return concept;

        //We check committed attributes first. In certain situations (adding the same attribute in multiple txs),
        //the ephemeral cache might be populated for a longer period of time.
        //As a result checking ephemeral attributes first might result in locking all the time.
        ConceptId attributeCommitted = attributeManager.attributesCommitted().getIfPresent(index);
        if (attributeCommitted != null) return getConcept(attributeCommitted);

        //check EPHA
        if (attributeManager.isAttributeEphemeral(index)) return null;

        //check graph
        return getConcept(Schema.VertexProperty.INDEX, index);
    }

    @Override
    public <T extends Concept> T getConcept(Schema.VertexProperty key, Object value) {
        VertexElement vertex = elementFactory.getVertexWithProperty(key, value);
        if (vertex != null) {
            return buildConcept(vertex);
        }
        return null;
    }

    public Set<Concept> getConcepts(Schema.VertexProperty key, Object value) {
        Set<Concept> concepts = new HashSet<>();
        Stream<VertexElement> vertices = elementFactory.getVerticesWithProperty(key, value);
        vertices.forEach(vertexElement -> concepts.add(buildConcept(vertexElement)));
        return concepts;
    }

    @Override
    public <T extends Concept> T getConcept(ConceptId conceptId) {
        if (!Schema.validateConceptId(conceptId)) {
            // fail fast if the concept Id format is invalid
            return null;
        }

        if (transactionCache.isConceptCached(conceptId)) {
            return transactionCache.getCachedConcept(conceptId);
        }

        // If edgeId, we are trying to fetch either:
        // - a concept edge
        // - a reified relation
        if (Schema.isEdgeId(conceptId)) {
            EdgeElement edgeElement = elementFactory.getEdgeElementWithId(Schema.elementId(conceptId));
            if (edgeElement != null) {
                return buildConcept(edgeElement);
            }
            // If element is still null,  it is possible we are referring to a ReifiedRelation which
            // uses its previous EdgeRelation as an id so property must be fetched
            return getConcept(Schema.VertexProperty.EDGE_RELATION_ID, conceptId.getValue());
        }

        Vertex vertex = elementFactory.getVertexWithId(Schema.elementId(conceptId));
        if (vertex == null) {
            return null;
        } else {
            return buildConcept(vertex);
        }
    }

    // TODO why are there three separate access methods here!
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        Schema.MetaSchema meta = Schema.MetaSchema.valueOf(label);
        if (meta != null) return getSchemaConcept(meta.getId());
        return getSchemaConcept(label, Schema.BaseType.SCHEMA_CONCEPT);
    }

    public <T extends SchemaConcept> T getSchemaConcept(Label label, Schema.BaseType baseType) {
        SchemaConcept schemaConcept;
        if (transactionCache.isTypeCached(label)) {
            schemaConcept = transactionCache.getCachedSchemaConcept(label);
        } else {
            schemaConcept = getSchemaConcept(convertToId(label));
        }
        return validateSchemaConcept(schemaConcept, baseType, () -> null);
    }

    @Override
    public <T extends SchemaConcept> T getSchemaConcept(LabelId id) {
        if (!id.isValid()) return null;
        return getConcept(Schema.VertexProperty.LABEL_ID, id.getValue());
    }

    @Override
    public AttributeType getMetaAttributeType() {
        return getSchemaConcept(Schema.MetaSchema.ATTRIBUTE.getId());
    }

    @Override
    public EntityType getMetaEntityType() {
        return getSchemaConcept(Schema.MetaSchema.ENTITY.getId());
    }

    @Override
    public RelationType getMetaRelationType() {
        return getSchemaConcept(Schema.MetaSchema.RELATION.getId());
    }

    @Override
    public Type getMetaConcept() {
        return getSchemaConcept(Schema.MetaSchema.THING.getId());
    }

    public Role getMetaRole() {
        return getSchemaConcept(Schema.MetaSchema.ROLE.getId());
    }

    @Override
    public Rule getMetaRule() {
        return getSchemaConcept(Schema.MetaSchema.RULE.getId());
    }

    @Override
    public <T extends Type> T getType(Label label) {
        return getSchemaConcept(label, Schema.BaseType.TYPE);
    }

    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ENTITY_TYPE);
    }

    @Override
    public RelationType getRelationType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.RELATION_TYPE);
    }

    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ATTRIBUTE_TYPE);
    }

    @Override
    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ROLE);
    }

    /**
     * This is used when assigning a type to a concept - we check the current shard.
     * The read vertex property contains the current shard vertex id.
     */
    public Shard getShardWithLock(String typeId) {
        Object currentShardId = elementFactory.getVertexWithId(typeId).property(Schema.VertexProperty.CURRENT_SHARD.name()).value();

        //Try to fetch the current shard vertex, because janus commits are not atomic, property might exists but the corresponding
        //vertex might not yet be created. Consequently the fetch might return null.
        Vertex shardVertex = elementFactory.getVertexWithId(currentShardId.toString());
        if (shardVertex != null) return elementFactory.getShard(shardVertex);

        //If current shard fetch fails. We pick any of the existing shards.
        Vertex typeVertex = elementFactory.getVertexWithId(typeId);
        return elementFactory.buildVertexElement(typeVertex).shards().findFirst().orElse(null);
    }

    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label), RULE);
    }


    /**
     * Converts a Type Label into a type Id for this specific graph. Mapping labels to ids will differ between graphs
     * so be sure to use the correct graph when performing the mapping.
     *
     * @param label The label to be converted to the id
     * @return The matching type id
     */
    @Override
    public LabelId convertToId(Label label) {
        if (transactionCache.isLabelCached(label)) {
            return transactionCache.convertLabelToId(label);
        }
        return LabelId.invalid();
    }


    private <T extends Concept> T validateSchemaConcept(Concept concept, Schema.BaseType baseType, Supplier<T> invalidHandler) {
        if (concept != null && baseType.getClassType().isInstance(concept)) {
            //noinspection unchecked
            return (T) concept;
        } else {
            return invalidHandler.get();
        }
    }


    /*

         --------  BUILD behaviors -------

     */

    /**
     * Constructors are called directly because this is only called when reading a known vertex or concept.
     * Thus tracking the concept can be skipped.
     *
     * @param vertex A vertex of an unknown type
     * @return A concept built to the correct type
     */
    @Override
    public <X extends Concept> X buildConcept(Vertex vertex) {
        return buildConcept(elementFactory.buildVertexElement(vertex));
    }

    @Override
    public <X extends Concept> X buildConcept(VertexElement vertexElement) {
        ConceptId conceptId = Schema.conceptId(vertexElement.element());
        Concept cachedConcept = transactionCache.getCachedConcept(conceptId);

        if (cachedConcept == null) {
            Schema.BaseType type;
            try {
                type = getBaseType(vertexElement);
            } catch (IllegalStateException e) {
                throw TemporaryWriteException.indexOverlap(vertexElement.element(), e);
            }
            Concept concept;
            switch (type) {
                case RELATION:
                    concept = new RelationImpl(new RelationReified(vertexElement, this, conceptNotificationChannel));
                    break;
                case TYPE:
                    concept = new TypeImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                case ROLE:
                    concept = new RoleImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                case RELATION_TYPE:
                    concept = new RelationTypeImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                case ENTITY:
                    concept = new EntityImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                case ENTITY_TYPE:
                    concept = new EntityTypeImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                case ATTRIBUTE_TYPE:
                    concept = new AttributeTypeImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                case ATTRIBUTE:
                    concept = new AttributeImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                case RULE:
                    concept = new RuleImpl(vertexElement, this, conceptNotificationChannel);
                    break;
                default:
                    throw GraknConceptException.unknownConceptType(type.name());
            }
            transactionCache.cacheConcept(concept);
            return (X) concept;
        }
        return (X) cachedConcept;
    }

    /**
     * Constructors are called directly because this is only called when reading a known Edge or Concept.
     * Thus tracking the concept can be skipped.
     *
     * @param edge A Edge of an unknown type
     * @return A concept built to the correct type
     */
    @Override
    public <X extends Concept> X buildConcept(Edge edge) {
        return buildConcept(elementFactory.buildEdgeElement(edge));
    }

    private <X extends Concept> X buildConcept(EdgeElement edgeElement) {
        Schema.EdgeLabel label = Schema.EdgeLabel.valueOf(edgeElement.label().toUpperCase(Locale.getDefault()));

        ConceptId conceptId = Schema.conceptId(edgeElement.element());
        if (!transactionCache.isConceptCached(conceptId)) {
            Concept concept;
            switch (label) {
                case ATTRIBUTE:
                    concept = new RelationImpl(new RelationEdge(edgeElement, this, conceptNotificationChannel));
                    break;
                default:
                    throw GraknConceptException.unknownConceptType(label.name());
            }
            transactionCache.cacheConcept(concept);
        }
        return transactionCache.getCachedConcept(conceptId);
    }


    /**
     * Used by RelationEdge to build a RelationImpl object out of a provided Edge
     * Build a concept around an prexisting edge that we may have cached
     */
    @Override
    public RelationImpl buildRelation(EdgeElement edge) {
        ConceptId conceptId = Schema.conceptId(edge.element());
        if (!transactionCache.isConceptCached(conceptId)) {
            RelationImpl relation = new RelationImpl(new RelationEdge(edge, this, conceptNotificationChannel));
            transactionCache.cacheConcept(relation);
            return relation;
        } else {
            return transactionCache.getCachedConcept(conceptId);
        }
    }


    /**
     * This is a helper method to get the base type of a vertex.
     * It first tried to get the base type via the label.
     * If this is not possible it then tries to get the base type via the Shard Edge.
     *
     * @param vertex The vertex to build a concept from
     * @return The base type of the vertex, if it is a valid concept.
     */
    private Schema.BaseType getBaseType(VertexElement vertex) {
        try {
            return Schema.BaseType.valueOf(vertex.label());
        } catch (IllegalArgumentException e) {
            //Base type appears to be invalid. Let's try getting the type via the shard edge
            Optional<VertexElement> type = vertex.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD).
                    map(EdgeElement::target).findAny();

            if (type.isPresent()) {
                String label = type.get().label();
                if (label.equals(Schema.BaseType.ENTITY_TYPE.name())) return Schema.BaseType.ENTITY;
                if (label.equals(RELATION_TYPE.name())) return Schema.BaseType.RELATION;
                if (label.equals(Schema.BaseType.ATTRIBUTE_TYPE.name())) return Schema.BaseType.ATTRIBUTE;
            }
        }
        throw new IllegalStateException("Could not determine the base type of vertex [" + vertex + "]");
    }
}