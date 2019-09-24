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

package grakn.core.server.kb.concept;

import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.LabelId;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.server.exception.TemporaryWriteException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.AbstractElement;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.session.ConceptObserver;
import grakn.core.server.session.cache.TransactionCache;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static grakn.core.server.kb.Schema.BaseType.RELATION_TYPE;

public class ConceptManager {

    private ElementFactory elementFactory;
    private TransactionCache transactionCache;
    private ConceptObserver conceptObserver; // only held to pass through to concepts
    private ReadWriteLock graphLock;

    public ConceptManager(ElementFactory elementFactory, TransactionCache transactionCache, ConceptObserver conceptObserver, ReadWriteLock graphLock) {
        this.elementFactory = elementFactory;
        this.transactionCache = transactionCache;
        this.conceptObserver = conceptObserver;
        this.graphLock = graphLock;
    }


    /*
        ----- CREATE behaviors for Schema concepts -----
     */

    /**
     * @param label             The Label of the SchemaConcept to create
     * @param baseType          The Schema.BaseType of the SchemaConcept to find or create
     * @param isImplicit        a flag indicating if the label we are creating is for an implicit grakn.core.concept.type.Type or not
     * @param newConceptFactory the factory to be using when creating a new SchemaConcept
     * @param <T>               The type of SchemaConcept to return
     * @return a new SchemaConcept
     */
    private <T extends SchemaConcept> T createSchemaConcept(Label label, Schema.BaseType baseType, boolean isImplicit, Function<VertexElement, T> newConceptFactory) {
        if (!isImplicit && label.getValue().startsWith(Schema.ImplicitType.RESERVED.getValue())) {
            throw TransactionException.invalidLabelStart(label);
        }

        VertexElement vertexElement = addTypeVertex(getNextId(), label, baseType);

        //Mark it as implicit here so we don't have to pass it down the constructors
        if (isImplicit) {
            vertexElement.property(Schema.VertexProperty.IS_IMPLICIT, true);
        }

        // noinspection unchecked
        return (T) SchemaConceptImpl.from(newConceptFactory.apply(vertexElement));
    }

    public EntityType createEntityType(Label label, EntityType metaEntityType) {
        return createSchemaConcept(label, Schema.BaseType.ENTITY_TYPE, false, v -> createEntityType(v, metaEntityType));
    }

    private EntityTypeImpl createEntityType(VertexElement vertex, EntityType type) {
        EntityTypeImpl entityType = EntityTypeImpl.create(vertex, type, this, conceptObserver);
        transactionCache.cacheConcept(entityType);
        return entityType;
    }

    public RelationType createRelationType(Label label, RelationType metaRelationType) {
        return createSchemaConcept(label, Schema.BaseType.RELATION_TYPE, false, v -> createRelationType(v, metaRelationType));
    }

    RelationType createImplicitRelationType(Label implicitRelationLabel) {
        return createSchemaConcept(implicitRelationLabel, Schema.BaseType.RELATION_TYPE, true,
                v -> createRelationType(v, getMetaRelationType()));
    }

    private RelationTypeImpl createRelationType(VertexElement vertex, RelationType type) {
        RelationTypeImpl relationType = RelationTypeImpl.create(vertex, type, this, conceptObserver);
        transactionCache.cacheConcept(relationType);
        return relationType;
    }

    public <V> AttributeType createAttributeType(Label label, AttributeType metaAttributeType, AttributeType.DataType<V> dataType) {
        return createSchemaConcept(label, Schema.BaseType.ATTRIBUTE_TYPE, false, v -> createAttributeType(v, metaAttributeType, dataType));
    }

    private <V> AttributeTypeImpl<V> createAttributeType(VertexElement vertex, AttributeType<V> type, AttributeType.DataType<V> dataType) {
        AttributeTypeImpl<V> attributeType = AttributeTypeImpl.create(vertex, type, dataType, this, conceptObserver);
        transactionCache.cacheConcept(attributeType);
        return attributeType;
    }

    public Role createRole(Label label, Role metaRole) {
        return createSchemaConcept(label, Schema.BaseType.ROLE, false, v -> createRole(v, metaRole));
    }

    Role createImplicitRole(Label implicitRoleLabel) {
        return createSchemaConcept(implicitRoleLabel, Schema.BaseType.ROLE, true,
                v -> createRole(v, getMetaRole()));
    }

    private RoleImpl createRole(VertexElement vertex, Role type) {
        RoleImpl role = RoleImpl.create(vertex, type, this, conceptObserver);
        transactionCache.cacheConcept(role);
        return role;
    }

    public Rule createRule(Label label, Pattern when, Pattern then, Rule metaRule) {
        return createSchemaConcept(label, Schema.BaseType.RULE, false, v -> createRule(v, metaRule, when, then));
    }

    private RuleImpl createRule(VertexElement vertex, Rule type, Pattern when, Pattern then) {
        RuleImpl rule = RuleImpl.create(vertex, type, when, then, this, conceptObserver);
        transactionCache.cacheConcept(rule);
        return rule;
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
        ------- CREATE behaviors for Concept instances ------
     */

    /**
     * Create a new attribute instance from a vertex, skip checking caches because this should be a brand new vertex
     * @param vertex - the new vertex to wrap in a Concept
     * @param type - the Concept type
     * @param persistedValue - value saved in the attribute
     * @param <V> - attribute type
     * @return - new Attribute Concept
     */
    <V> AttributeImpl<V> createAttribute(VertexElement vertex, AttributeType<V> type, V persistedValue) {
        return AttributeImpl.create(vertex, type, persistedValue, this, conceptObserver);
    }

    /**
     * Create a new Relation instance from an edge
     * Skip checking caches because this should be a brand new edge and concept
     */
    RelationImpl createRelation(EdgeElement edge, RelationType type, Role owner, Role value) {
        return RelationImpl.get(RelationEdge.create(type, owner, value, edge, this, conceptObserver));
    }

    /**
     * Create a new Relation instance from a vertex
     * Skip checking caches because this should be a brand new vertex and concept
     */
    RelationImpl createRelation(VertexElement vertex, RelationType type) {
        return RelationImpl.get(RelationReified.create(vertex, type, this, conceptObserver));
    }

    /**
     * Create a new Entity instance from a vertex
     * Skip checking caches because this should be a brand new vertex and concept
     */
    EntityImpl createEntity(VertexElement vertex, EntityType type) {
        return EntityImpl.create(vertex, type, this, conceptObserver);
    }





    // ---------- RETRIEVE behaviors ------

    /**
     * Check the transaction cache to see if we have the attribute already by index
     * return NULL if attribtue does not exist in cache
     */
    @Nullable
    Attribute getCachedAttribute(String index) {
        Attribute concept = transactionCache.getAttributeCache().get(index);
        return concept;
    }

    /**
     * This is only used when checking if attribute exists before trying to create a new one.
     * We use a readLock as janusGraph commit does not seem to be atomic. Further investigation needed
     */
    Attribute getAttributeWithLock(String index) {
        Attribute concept = getCachedAttribute(index);
        if (concept != null) return concept;

        graphLock.readLock().lock();
        try {
            return getConcept(Schema.VertexProperty.INDEX, index);
        } finally {
            graphLock.readLock().unlock();
        }
    }

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

    public <T extends Concept> T getConcept(ConceptId conceptId) {
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

    public <T extends SchemaConcept> T getSchemaConcept(LabelId id) {
        if (!id.isValid()) return null;
        return getConcept(Schema.VertexProperty.LABEL_ID, id.getValue());
    }

    AttributeType getMetaAttributeType() {
        return getSchemaConcept(Label.of(Graql.Token.Type.ATTRIBUTE.toString()));
    }

    private RelationType getMetaRelationType() {
        return getSchemaConcept(Label.of(Graql.Token.Type.RELATION.toString()));
    }


    private Type getMetaConcept() {
        return getSchemaConcept(Label.of(Graql.Token.Type.THING.toString()));
    }

    private Role getMetaRole() {
        return getSchemaConcept(Label.of(Graql.Token.Type.ROLE.toString()));
    }

    public <T extends grakn.core.concept.type.Type> T getType(Label label) {
        return getSchemaConcept(label, Schema.BaseType.TYPE);
    }

    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ENTITY_TYPE);
    }

    public RelationType getRelationType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.RELATION_TYPE);
    }

    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ATTRIBUTE_TYPE);
    }

    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.ROLE);
    }

    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label), Schema.BaseType.RULE);
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


    private <T extends Concept> T validateSchemaConcept(Concept concept, Schema.BaseType baseType, Supplier<T> invalidHandler) {
        if (concept != null && baseType.getClassType().isInstance(concept)) {
            //noinspection unchecked
            return (T) concept;
        } else {
            return invalidHandler.get();
        }
    }



    /*
     --------  BUILD behaviors ------
     */

    /**
     * Constructors are called directly because this is only called when reading a known vertex or concept.
     * Thus tracking the concept can be skipped.
     *
     * @param vertex A vertex of an unknown type
     * @return A concept built to the correct type
     */
    public <X extends Concept> X buildConcept(Vertex vertex) {
        return buildConcept(elementFactory.buildVertexElement(vertex));
    }

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
                    concept = RelationImpl.get(RelationReified.get(vertexElement, this, conceptObserver));
                    break;
                case TYPE:
                    concept = TypeImpl.get(vertexElement, this, conceptObserver);
                    break;
                case ROLE:
                    concept = RoleImpl.get(vertexElement, this, conceptObserver);
                    break;
                case RELATION_TYPE:
                    concept = RelationTypeImpl.get(vertexElement, this, conceptObserver);
                    break;
                case ENTITY:
                    concept = EntityImpl.get(vertexElement, this, conceptObserver);
                    break;
                case ENTITY_TYPE:
                    concept = EntityTypeImpl.get(vertexElement, this, conceptObserver);
                    break;
                case ATTRIBUTE_TYPE:
                    concept = AttributeTypeImpl.get(vertexElement, this, conceptObserver);
                    break;
                case ATTRIBUTE:
                    concept = AttributeImpl.get(vertexElement, this, conceptObserver);
                    break;
                case RULE:
                    concept = RuleImpl.get(vertexElement, this, conceptObserver);
                    break;
                default:
                    throw TransactionException.unknownConcept(type.name());
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
    public <X extends Concept> X buildConcept(Edge edge) {
        return buildConcept(elementFactory.buildEdgeElement(edge));
    }

    public <X extends Concept> X buildConcept(EdgeElement edgeElement) {
        Schema.EdgeLabel label = Schema.EdgeLabel.valueOf(edgeElement.label().toUpperCase(Locale.getDefault()));

        ConceptId conceptId = Schema.conceptId(edgeElement.element());
        if (!transactionCache.isConceptCached(conceptId)) {
            Concept concept;
            switch (label) {
                case ATTRIBUTE:
                    concept = RelationImpl.get(RelationEdge.get(edgeElement, this, conceptObserver));
                    break;
                default:
                    throw TransactionException.unknownConcept(label.name());
            }
            transactionCache.cacheConcept(concept);
        }
        return transactionCache.getCachedConcept(conceptId);
    }


    /**
     * Used by RelationEdge to build a RelationImpl object out of a provided Edge
     * Build a concept around an prexisting edge that we may have cached
     */
    RelationImpl buildRelation(EdgeElement edge) {
        ConceptId conceptId = Schema.conceptId(edge.element());
        if (!transactionCache.isConceptCached(conceptId)) {
            RelationImpl relation = RelationImpl.get(RelationEdge.get(edge, this, conceptObserver));
            transactionCache.cacheConcept(relation);
            return relation;
        } else {
            return transactionCache.getCachedConcept(conceptId);
        }
    }

    /**
     * Used by RelationEdge when it needs to reify a relation.
     * @return ReifiedRelation
     */
    RelationReified buildRelationReified(VertexElement vertex, RelationType type) {
        return RelationReified.create(vertex, type, this, conceptObserver);
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