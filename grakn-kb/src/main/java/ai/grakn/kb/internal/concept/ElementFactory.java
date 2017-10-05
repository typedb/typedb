/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.graql.Pattern;
import ai.grakn.kb.internal.GraknTxAbstract;
import ai.grakn.kb.internal.structure.AbstractElement;
import ai.grakn.kb.internal.structure.Casting;
import ai.grakn.kb.internal.structure.EdgeElement;
import ai.grakn.kb.internal.structure.Shard;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static ai.grakn.util.Schema.BaseType.RELATIONSHIP_TYPE;

/**
 * <p>
 *     Constructs Concepts And Edges
 * </p>
 *
 * <p>
 *     This class turns Tinkerpop {@link Vertex} and {@link org.apache.tinkerpop.gremlin.structure.Edge}
 *     into Grakn {@link Concept} and {@link EdgeElement}.
 *
 *     Construction is only successful if the vertex and edge properties contain the needed information.
 *     A concept must include a label which is a {@link ai.grakn.util.Schema.BaseType}.
 *     An edge must include a label which is a {@link ai.grakn.util.Schema.EdgeLabel}.
 * </p>
 *
 * @author fppt
 */
public final class ElementFactory {
    private final Logger LOG = LoggerFactory.getLogger(ElementFactory.class);
    private final GraknTxAbstract tx;

    public ElementFactory(GraknTxAbstract tx){
        this.tx = tx;
    }

    private <X extends Concept, E extends AbstractElement> X getOrBuildConcept(E element, ConceptId conceptId, Function<E, X> conceptBuilder){
        if(!tx.txCache().isConceptCached(conceptId)){
            X newConcept = conceptBuilder.apply(element);
            tx.txCache().cacheConcept(newConcept);
        }
        return tx.txCache().getCachedConcept(conceptId);
    }

    private <X extends Concept> X getOrBuildConcept(VertexElement element, Function<VertexElement, X> conceptBuilder){
        ConceptId conceptId = ConceptId.of(element.property(Schema.VertexProperty.ID));
        return getOrBuildConcept(element, conceptId, conceptBuilder);
    }

    private <X extends Concept> X getOrBuildConcept(EdgeElement element, Function<EdgeElement, X> conceptBuilder){
        ConceptId conceptId = ConceptId.of(element.id().getValue());
        return getOrBuildConcept(element, conceptId, conceptBuilder);
    }

    // ---------------------------------------- Building Attribute Types  -----------------------------------------------
    public <V> AttributeTypeImpl<V> buildResourceType(VertexElement vertex, AttributeType<V> type, AttributeType.DataType<V> dataType){
        return getOrBuildConcept(vertex, (v) -> AttributeTypeImpl.create(v, type, dataType));
    }

    // ------------------------------------------ Building Resources
    <V> AttributeImpl<V> buildResource(VertexElement vertex, AttributeType<V> type, Object persitedValue){
        return getOrBuildConcept(vertex, (v) -> AttributeImpl.create(v, type, persitedValue));
    }

    // ---------------------------------------- Building Relationship Types  -----------------------------------------------
    public RelationshipTypeImpl buildRelationType(VertexElement vertex, RelationshipType type){
        return getOrBuildConcept(vertex, (v) -> RelationshipTypeImpl.create(v, type));
    }

    // -------------------------------------------- Building Relations
    RelationshipImpl buildRelation(VertexElement vertex, RelationshipType type){
        return getOrBuildConcept(vertex, (v) -> RelationshipImpl.create(buildRelationReified(v, type)));
    }
    public RelationshipImpl buildRelation(EdgeElement edge, RelationshipType type, Role owner, Role value){
        return getOrBuildConcept(edge, (e) -> RelationshipImpl.create(RelationshipEdge.create(type, owner, value, edge)));
    }
    RelationshipImpl buildRelation(EdgeElement edge){
        return getOrBuildConcept(edge, (e) -> RelationshipImpl.create(RelationshipEdge.get(edge)));
    }
    RelationshipReified buildRelationReified(VertexElement vertex, RelationshipType type){
        return RelationshipReified.create(vertex, type);
    }

    // ----------------------------------------- Building Entity Types  ------------------------------------------------
    public EntityTypeImpl buildEntityType(VertexElement vertex, EntityType type){
        return getOrBuildConcept(vertex, (v) -> EntityTypeImpl.create(v, type));
    }

    // ------------------------------------------- Building Entities
    EntityImpl buildEntity(VertexElement vertex, EntityType type){
        return getOrBuildConcept(vertex, (v) -> EntityImpl.create(v, type));
    }

    // ----------------------------------------- Building Rules --------------------------------------------------
    public RuleImpl buildRule(VertexElement vertex, Rule type, Pattern when, Pattern then){
        return getOrBuildConcept(vertex, (v) -> RuleImpl.create(v, type, when, then));
    }

    // ------------------------------------------ Building Roles  Types ------------------------------------------------
    public RoleImpl buildRole(VertexElement vertex, Role type){
        return getOrBuildConcept(vertex, (v) -> RoleImpl.create(v, type));
    }

    /**
     * Constructors are called directly because this is only called when reading a known vertex or concept.
     * Thus tracking the concept can be skipped.
     *
     * @param vertex A vertex of an unknown type
     * @return A concept built to the correct type
     */
    public <X extends Concept> Optional<X> buildConcept(Vertex vertex){
        return buildVertexElement(vertex).flatMap(this::buildConcept);
    }

    public <X extends Concept> Optional<X> buildConcept(VertexElement vertexElement){
        Schema.BaseType type;

        try {
            type = getBaseType(vertexElement);
        } catch (IllegalStateException e){
            LOG.warn("Invalid vertex [" + vertexElement + "] due to " + e.getMessage(), e);
            return Optional.empty();
        }

        ConceptId conceptId = ConceptId.of(vertexElement.property(Schema.VertexProperty.ID));
        if(!tx.txCache().isConceptCached(conceptId)){
            Concept concept;
            switch (type) {
                case RELATIONSHIP:
                    concept = RelationshipImpl.create(RelationshipReified.get(vertexElement));
                    break;
                case TYPE:
                    concept = new TypeImpl(vertexElement);
                    break;
                case ROLE:
                    concept = RoleImpl.get(vertexElement);
                    break;
                case RELATIONSHIP_TYPE:
                    concept = RelationshipTypeImpl.get(vertexElement);
                    break;
                case ENTITY:
                    concept = EntityImpl.get(vertexElement);
                    break;
                case ENTITY_TYPE:
                    concept = EntityTypeImpl.get(vertexElement);
                    break;
                case ATTRIBUTE_TYPE:
                    concept = AttributeTypeImpl.get(vertexElement);
                    break;
                case ATTRIBUTE:
                    concept = AttributeImpl.get(vertexElement);
                    break;
                case RULE:
                    concept = RuleImpl.get(vertexElement);
                    break;
                default:
                    throw GraknTxOperationException.unknownConcept(type.name());
            }
            tx.txCache().cacheConcept(concept);
        }
        return Optional.of(tx.txCache().getCachedConcept(conceptId));
    }

    /**
     * Constructors are called directly because this is only called when reading a known {@link Edge} or {@link Concept}.
     * Thus tracking the concept can be skipped.
     *
     * @param edge A {@link Edge} of an unknown type
     * @return A concept built to the correct type
     */
    public <X extends Concept> Optional<X> buildConcept(Edge edge){
        return buildConcept(buildEdgeElement(edge));
    }

    public <X extends Concept> Optional<X> buildConcept(EdgeElement edgeElement){
        Schema.EdgeLabel label;

        try {
            label = Schema.EdgeLabel.valueOf(edgeElement.label().toUpperCase(Locale.getDefault()));
        } catch (IllegalStateException e){
            LOG.warn("Invalid edge [" + edgeElement + "] due to " + e.getMessage(), e);
            return Optional.empty();
        }


        ConceptId conceptId = ConceptId.of(edgeElement.id().getValue());
        if(!tx.txCache().isConceptCached(conceptId)){
            Concept concept;
            switch (label) {
                case ATTRIBUTE:
                    concept = RelationshipImpl.create(RelationshipEdge.get(edgeElement));
                    break;
                default:
                    throw GraknTxOperationException.unknownConcept(label.name());
            }
            tx.txCache().cacheConcept(concept);
        }
        return Optional.of(tx.txCache().getCachedConcept(conceptId));
    }

    /**
     * This is a helper method to get the base type of a vertex.
     * It first tried to get the base type via the label.
     * If this is not possible it then tries to get the base type via the Shard Edge.
     *
     * @param vertex The vertex to build a concept from
     * @return The base type of the vertex, if it is a valid concept.
     */
    private Schema.BaseType getBaseType(VertexElement vertex){
        try {
            return Schema.BaseType.valueOf(vertex.label());
        } catch (IllegalArgumentException e){
            //Base type appears to be invalid. Let's try getting the type via the shard edge
            Optional<VertexElement> type = vertex.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHARD).
                    map(EdgeElement::target).
                    flatMap(CommonUtil::optionalToStream).
                    findAny();

            if(type.isPresent()){
                String label = type.get().label();
                if(label.equals(Schema.BaseType.ENTITY_TYPE.name())) return Schema.BaseType.ENTITY;
                if(label.equals(RELATIONSHIP_TYPE.name())) return Schema.BaseType.RELATIONSHIP;
                if(label.equals(Schema.BaseType.ATTRIBUTE_TYPE.name())) return Schema.BaseType.ATTRIBUTE;
            }
        }
        throw new IllegalStateException("Could not determine the base type of vertex [" + vertex + "]");
    }

    // ---------------------------------------- Non Concept Construction -----------------------------------------------
    public EdgeElement buildEdgeElement(Edge edge){
        return new EdgeElement(tx, edge);
    }

    Casting buildCasting(Edge edge){
        return buildCasting(buildEdgeElement(edge));
    }

    public Casting buildCasting(EdgeElement edge) {
        return new Casting(edge);
    }

    Shard buildShard(ConceptImpl shardOwner, VertexElement vertexElement){
        return new Shard(shardOwner, vertexElement);
    }

    Shard buildShard(VertexElement vertexElement){
        return new Shard(vertexElement);
    }

    Optional<Shard> buildShard(Vertex vertex){
        return buildVertexElement(vertex).map(Shard::new);
    }

    /**
     * Builds a {@link VertexElement} from an already existing Vertex. An empty optional is returned if the passed in
     * vertex is not valid. A vertex is not valid if it is null or has been deleted
     *
     * @param vertex A vertex which can possibly be turned into a {@link VertexElement}
     * @return A {@link VertexElement} of
     */
    public Optional<VertexElement> buildVertexElement(Vertex vertex){
        if(tx.validElement(vertex)) {
            return Optional.of(new VertexElement(tx, vertex));
        } else{
            LOG.warn("Invalid vertex [" + vertex + "]");
            return Optional.empty();
        }
    }

    /**
     * Creates a new {@link VertexElement} with a {@link ConceptId} which can optionally be set.
     *
     * @param baseType The {@link Schema.BaseType}
     * @param conceptIds the optional {@link ConceptId} to set as the new {@link ConceptId}
     * @return a new {@link VertexElement}
     */
    public VertexElement addVertexElement(Schema.BaseType baseType, ConceptId ... conceptIds) {
        Vertex vertex = tx.getTinkerPopGraph().addVertex(baseType.name());
        String newConceptId = Schema.PREFIX_VERTEX + vertex.id().toString();
        if(conceptIds.length > 1){
            throw new IllegalArgumentException("Cannot provide more than one concept id when creating a new concept");
        } else if (conceptIds.length == 1){
            newConceptId = conceptIds[0].getValue();
        }
        vertex.property(Schema.VertexProperty.ID.name(), newConceptId);
        return new VertexElement(tx, vertex);
    }
}
