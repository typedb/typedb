package grakn.core.server.kb.concept;

import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.LabelId;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.server.exception.TemporaryWriteException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.AbstractElement;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.session.cache.TransactionCache;
import graql.lang.pattern.Pattern;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static grakn.core.server.kb.Schema.BaseType.RELATION_TYPE;

public class ConceptManager {

    private ElementFactory elementFactory;
    private TransactionCache transactionCache;

    public ConceptManager(ElementFactory elementFactory, TransactionCache transactionCache) {
        this.elementFactory = elementFactory;
        this.transactionCache = transactionCache;
    }


    /*
    ---- capabilities migrated from Transaction for now
     */

    public <T extends Concept> T getConcept(Schema.VertexProperty key, Object value) {
        Vertex vertex = elementFactory.getVertexWithProperty(key, value);
        if (vertex != null) {
            return buildConcept(vertex);
        }
        return null;
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

        return buildConcept(elementFactory.getVertexWithId(Schema.elementId(conceptId)));
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
    private LabelId convertToId(Label label) {
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

     -------- end
     */


    private <X extends Concept, E extends AbstractElement> X getOrBuildConcept(E element, ConceptId conceptId, Function<E, X> conceptBuilder) {
        if (!transactionCache.isConceptCached(conceptId)) {
            X newConcept = conceptBuilder.apply(element);
            transactionCache.cacheConcept(newConcept);
        }
        return transactionCache.getCachedConcept(conceptId);
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
        return getOrBuildConcept(vertex, (v) -> AttributeTypeImpl.create(v, type, dataType, this, transactionCache));
    }

    // ------------------------------------------ Building Attribute
    <V> AttributeImpl<V> buildAttribute(VertexElement vertex, AttributeType<V> type, V persistedValue) {
        return getOrBuildConcept(vertex, (v) -> AttributeImpl.create(v, type, persistedValue, this, transactionCache));
    }

    // ---------------------------------------- Building Relation Types  -----------------------------------------------
    public RelationTypeImpl buildRelationType(VertexElement vertex, RelationType type) {
        return getOrBuildConcept(vertex, (v) -> RelationTypeImpl.create(v, type, this, transactionCache));
    }

    // -------------------------------------------- Building Relations


    /**
     * Used to build a RelationEdge by ThingImpl when it needs to connect itself with an attribute (implicit relation)
     */
    RelationImpl buildRelation(EdgeElement edge, RelationType type, Role owner, Role value) {
        return getOrBuildConcept(edge, (e) -> RelationImpl.create(RelationEdge.create(type, owner, value, edge, this, transactionCache)));
    }

    /**
     * Used by RelationEdge to build a RelationImpl object out of a provided Edge
     */
    RelationImpl buildRelation(EdgeElement edge) {
        return getOrBuildConcept(edge, (e) -> RelationImpl.create(RelationEdge.get(edge, this, transactionCache)));
    }

    /**
     * Used by RelationEdge when it needs to reify a relation.
     * Used by this factory when need to build an explicit relation
     *
     * @return ReifiedRelation
     */
    RelationReified buildRelationReified(VertexElement vertex, RelationType type) {
        return RelationReified.create(vertex, type, this, transactionCache);
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
        return getOrBuildConcept(vertex, (v) -> EntityTypeImpl.create(v, type, this, transactionCache));
    }

    // ------------------------------------------- Building Entities
    EntityImpl buildEntity(VertexElement vertex, EntityType type) {
        return getOrBuildConcept(vertex, (v) -> EntityImpl.create(v, type, this, transactionCache));
    }

    // ----------------------------------------- Building Rules --------------------------------------------------
    public RuleImpl buildRule(VertexElement vertex, Rule type, Pattern when, Pattern then) {
        return getOrBuildConcept(vertex, (v) -> RuleImpl.create(v, type, when, then, this, transactionCache));
    }

    // ------------------------------------------ Building Roles  Types ------------------------------------------------
    public RoleImpl buildRole(VertexElement vertex, Role type) {
        return getOrBuildConcept(vertex, (v) -> RoleImpl.create(v, type, this, transactionCache));
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
                    concept = RelationImpl.create(RelationReified.get(vertexElement, this, transactionCache));
                    break;
                case TYPE:
                    concept = new TypeImpl(vertexElement, this, transactionCache);
                    break;
                case ROLE:
                    concept = RoleImpl.get(vertexElement, this, transactionCache);
                    break;
                case RELATION_TYPE:
                    concept = RelationTypeImpl.get(vertexElement, this, transactionCache);
                    break;
                case ENTITY:
                    concept = EntityImpl.get(vertexElement, this, transactionCache);
                    break;
                case ENTITY_TYPE:
                    concept = EntityTypeImpl.get(vertexElement, this, transactionCache);
                    break;
                case ATTRIBUTE_TYPE:
                    concept = AttributeTypeImpl.get(vertexElement, this, transactionCache);
                    break;
                case ATTRIBUTE:
                    concept = AttributeImpl.get(vertexElement, this, transactionCache);
                    break;
                case RULE:
                    concept = RuleImpl.get(vertexElement, this, transactionCache);
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
                    concept = RelationImpl.create(RelationEdge.get(edgeElement, this, transactionCache));
                    break;
                default:
                    throw TransactionException.unknownConcept(label.name());
            }
            transactionCache.cacheConcept(concept);
        }
        return transactionCache.getCachedConcept(conceptId);
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