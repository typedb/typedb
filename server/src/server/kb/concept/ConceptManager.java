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
import grakn.core.server.exception.PropertyNotUniqueException;
import grakn.core.server.exception.TemporaryWriteException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.AbstractElement;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.session.TransactionDataContainer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static grakn.core.server.kb.Schema.BaseType.RELATION_TYPE;

public class ConceptManager {

    private ElementFactory elementFactory;
    private TransactionDataContainer transactionDataContainer;
    private ReadWriteLock graphLock;

    public ConceptManager(ElementFactory elementFactory, TransactionDataContainer transactionDataContainer, ReadWriteLock graphLock) {
        this.elementFactory = elementFactory;
        this.transactionDataContainer = transactionDataContainer;
        this.graphLock = graphLock;
    }


    /*
    ---- capabilities migrated from Transaction for now
     */

    // ------ PUT

    public Role putRoleTypeImplicit(Label implicitRoleLabel) {
        return putSchemaConcept(implicitRoleLabel, Schema.BaseType.ROLE, true,
                v -> buildRole(v, getMetaRole()));
    }

    public RelationType putRelationTypeImplicit(Label implicitRelationLabel) {
        return putSchemaConcept(implicitRelationLabel, Schema.BaseType.RELATION_TYPE, true,
                v -> buildRelationType(v, getMetaRelationType()));
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
    public <T extends SchemaConcept> T putSchemaConcept(Label label, Schema.BaseType baseType, boolean isImplicit, Function<VertexElement, T> newConceptFactory) {
        //Get the type if it already exists otherwise build a new one
        SchemaConceptImpl schemaConcept = getSchemaConcept(label);
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
     * Adds a new type vertex which occupies a grakn id. This result in the grakn id count on the meta concept to be
     * incremented.
     *
     * @param label    The label of the new type vertex
     * @param baseType The base type of the new type
     * @return The new type vertex
     *
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
     * Throws an exception when adding a SchemaConcept using a Label which is already taken
     */
    private TransactionException labelTaken(SchemaConcept schemaConcept) {
        if (Schema.MetaSchema.isMetaLabel(schemaConcept.label())) {
            return TransactionException.reservedLabel(schemaConcept.label());
        }
        return PropertyNotUniqueException.cannotCreateProperty(schemaConcept, Schema.VertexProperty.SCHEMA_LABEL, schemaConcept.label());
    }

    /**
     * This is only used when checking if attribute exists before trying to create a new one.
     * We use a readLock as janusGraph commit does not seem to be atomic. Further investigation needed
     */
    public Attribute getAttributeWithLock(String index) {
        Attribute concept = transactionDataContainer.transactionCache().getAttributeCache().get(index);
        if (concept != null) return concept;

        graphLock.readLock().lock();
        try {
            return getConcept(Schema.VertexProperty.INDEX, index);
        } finally {
            graphLock.readLock().unlock();
        }
    }

    // ---------- GET


    public <T extends Concept> T getConcept(Schema.VertexProperty key, Object value) {
        Vertex vertex = elementFactory.getVertexWithProperty(key, value);
        if (vertex != null) {
            return buildConcept(vertex);
        }
        return null;
    }

    public <T extends Concept> T getConcept(ConceptId conceptId) {
        if (transactionDataContainer.transactionCache().isConceptCached(conceptId)) {
            return transactionDataContainer.transactionCache().getCachedConcept(conceptId);
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
        if (transactionDataContainer.transactionCache().isTypeCached(label)) {
            schemaConcept = transactionDataContainer.transactionCache().getCachedSchemaConcept(label);
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
        if (transactionDataContainer.transactionCache().isLabelCached(label)) {
            return transactionDataContainer.transactionCache().convertLabelToId(label);
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

     -------- end
     */


    private <X extends Concept, E extends AbstractElement> X getOrBuildConcept(E element, ConceptId conceptId, Function<E, X> conceptBuilder) {
        if (!transactionDataContainer.transactionCache().isConceptCached(conceptId)) {
            X newConcept = conceptBuilder.apply(element);
            transactionDataContainer.transactionCache().cacheConcept(newConcept);
        }
        return transactionDataContainer.transactionCache().getCachedConcept(conceptId);
    }

    private <X extends Concept> X getOrBuildConcept(VertexElement element, Function<VertexElement, X> conceptBuilder) {
        ConceptId conceptId = Schema.conceptId(element.element());
        return getOrBuildConcept(element, conceptId, conceptBuilder);
    }

    private <X extends Concept> X getOrBuildConcept(EdgeElement element, Function<EdgeElement, X> conceptBuilder) {
        ConceptId conceptId = Schema.conceptId(element.element());
        return getOrBuildConcept(element, conceptId, conceptBuilder);
    }

    // ---------------------------------------- Building Attribute Types  -----------------------------------------------
    public <V> AttributeTypeImpl<V> buildAttributeType(VertexElement vertex, AttributeType<V> type, AttributeType.DataType<V> dataType) {
        return getOrBuildConcept(vertex, (v) -> AttributeTypeImpl.create(v, type, dataType, this, transactionDataContainer));
    }

    // ------------------------------------------ Building Attribute
    <V> AttributeImpl<V> buildAttribute(VertexElement vertex, AttributeType<V> type, V persistedValue) {
        return getOrBuildConcept(vertex, (v) -> AttributeImpl.create(v, type, persistedValue, this, transactionDataContainer));
    }

    // ---------------------------------------- Building Relation Types  -----------------------------------------------
    public RelationTypeImpl buildRelationType(VertexElement vertex, RelationType type) {
        return getOrBuildConcept(vertex, (v) -> RelationTypeImpl.create(v, type, this, transactionDataContainer));
    }

    // -------------------------------------------- Building Relations


    /**
     * Used to build a RelationEdge by ThingImpl when it needs to connect itself with an attribute (implicit relation)
     */
    RelationImpl buildRelation(EdgeElement edge, RelationType type, Role owner, Role value) {
        return getOrBuildConcept(edge, (e) -> RelationImpl.create(RelationEdge.create(type, owner, value, edge, this, transactionDataContainer)));
    }

    /**
     * Used by RelationEdge to build a RelationImpl object out of a provided Edge
     */
    RelationImpl buildRelation(EdgeElement edge) {
        return getOrBuildConcept(edge, (e) -> RelationImpl.create(RelationEdge.get(edge, this, transactionDataContainer)));
    }

    /**
     * Used by RelationEdge when it needs to reify a relation.
     * Used by this factory when need to build an explicit relation
     *
     * @return ReifiedRelation
     */
    RelationReified buildRelationReified(VertexElement vertex, RelationType type) {
        return RelationReified.create(vertex, type, this, transactionDataContainer);
    }

    /**
     * Used by RelationTypeImpl to create a new instance of RelationImpl
     * first build a ReifiedRelation and then inject it to RelationImpl
     *
     * @return
     */
    RelationImpl buildRelation(VertexElement vertex, RelationType type) {
        return getOrBuildConcept(vertex, (v) -> RelationImpl.create(buildRelationReified(v, type)));
    }

    // ----------------------------------------- Building Entity Types  ------------------------------------------------
    public EntityTypeImpl buildEntityType(VertexElement vertex, EntityType type) {
        return getOrBuildConcept(vertex, (v) -> EntityTypeImpl.create(v, type, this, transactionDataContainer));
    }

    // ------------------------------------------- Building Entities
    EntityImpl buildEntity(VertexElement vertex, EntityType type) {
        return getOrBuildConcept(vertex, (v) -> EntityImpl.create(v, type, this, transactionDataContainer));
    }

    // ----------------------------------------- Building Rules --------------------------------------------------
    public RuleImpl buildRule(VertexElement vertex, Rule type, Pattern when, Pattern then) {
        return getOrBuildConcept(vertex, (v) -> RuleImpl.create(v, type, when, then, this, transactionDataContainer));
    }

    // ------------------------------------------ Building Roles  Types ------------------------------------------------
    public RoleImpl buildRole(VertexElement vertex, Role type) {
        return getOrBuildConcept(vertex, (v) -> RoleImpl.create(v, type, this, transactionDataContainer));
    }

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
        Concept cachedConcept = transactionDataContainer.transactionCache().getCachedConcept(conceptId);

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
                    concept = RelationImpl.create(RelationReified.get(vertexElement, this, transactionDataContainer));
                    break;
                case TYPE:
                    concept = new TypeImpl(vertexElement, this, transactionDataContainer);
                    break;
                case ROLE:
                    concept = RoleImpl.get(vertexElement, this, transactionDataContainer);
                    break;
                case RELATION_TYPE:
                    concept = RelationTypeImpl.get(vertexElement, this, transactionDataContainer);
                    break;
                case ENTITY:
                    concept = EntityImpl.get(vertexElement, this, transactionDataContainer);
                    break;
                case ENTITY_TYPE:
                    concept = EntityTypeImpl.get(vertexElement, this, transactionDataContainer);
                    break;
                case ATTRIBUTE_TYPE:
                    concept = AttributeTypeImpl.get(vertexElement, this, transactionDataContainer);
                    break;
                case ATTRIBUTE:
                    concept = AttributeImpl.get(vertexElement, this, transactionDataContainer);
                    break;
                case RULE:
                    concept = RuleImpl.get(vertexElement, this, transactionDataContainer);
                    break;
                default:
                    throw TransactionException.unknownConcept(type.name());
            }
            transactionDataContainer.transactionCache().cacheConcept(concept);
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
        if (!transactionDataContainer.transactionCache().isConceptCached(conceptId)) {
            Concept concept;
            switch (label) {
                case ATTRIBUTE:
                    concept = RelationImpl.create(RelationEdge.get(edgeElement, this, transactionDataContainer));
                    break;
                default:
                    throw TransactionException.unknownConcept(label.name());
            }
            transactionDataContainer.transactionCache().cacheConcept(concept);
        }
        return transactionDataContainer.transactionCache().getCachedConcept(conceptId);
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