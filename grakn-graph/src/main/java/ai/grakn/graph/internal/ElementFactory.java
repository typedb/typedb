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

package ai.grakn.graph.internal;

import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Internal factory to produce different types of concepts
 */
final class ElementFactory {
    private final Logger LOG = LoggerFactory.getLogger(ElementFactory.class);
    private final AbstractGraknGraph graknGraph;

    ElementFactory(AbstractGraknGraph graknGraph){
        this.graknGraph = graknGraph;
    }

    // ------------------------------------------- Building Castings  --------------------------------------------------
    CastingImpl buildCasting(Vertex v, RoleType type){
        return buildCasting(v, Optional.of(type));
    }
    private CastingImpl buildCasting(Vertex v, Optional<RoleType> type){
        return new CastingImpl(graknGraph, v, type);
    }

    // -------------------------------------------- Building Types  ----------------------------------------------------
    private TypeImpl buildType(Vertex v, Optional<Type> type, Optional<Boolean> isImplicit){
        return new TypeImpl(graknGraph, v, type, isImplicit);
    }

    // ---------------------------------------- Building Resource Types  -----------------------------------------------
    <V> ResourceTypeImpl<V> buildResourceType(Vertex v, ResourceType type, ResourceType.DataType<V> dataType, Boolean isUnique){
        return buildResourceType(v, Optional.of(type), Optional.of(dataType), Optional.of(isUnique));
    }
    private <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Optional<ResourceType> type, Optional<ResourceType.DataType<V>> dataType, Optional<Boolean> isUnique){
        return new ResourceTypeImpl(graknGraph, v, type, dataType, isUnique);
    }

    // ------------------------------------------ Building Resources
    <V> ResourceImpl <V> buildResource(Vertex v, ResourceType<V> type, V value){
        return buildResource(v, Optional.of(type), Optional.of(value));
    }
    private <V> ResourceImpl <V> buildResource(Vertex v, Optional<ResourceType<V>> type, Optional<V> value){
        return new ResourceImpl<>(graknGraph, v, type, value);
    }

    // ---------------------------------------- Building Relation Types  -----------------------------------------------
    RelationTypeImpl buildRelationType(Vertex v, RelationType type, Boolean isImplicit){
        return buildRelationType(v, Optional.of(type), Optional.of(isImplicit));
    }
    private RelationTypeImpl buildRelationType(Vertex v, Optional<RelationType> type, Optional<Boolean> isImplicit){
        return new RelationTypeImpl(graknGraph, v, type, isImplicit);
    }

    // -------------------------------------------- Building Relations
    RelationImpl buildRelation(Vertex v, RelationType type){
        return buildRelation(v, Optional.of(type));
    }
    private RelationImpl buildRelation(Vertex v, Optional<RelationType> type){
        return new RelationImpl(graknGraph, v, type);
    }

    // ----------------------------------------- Building Entity Types  ------------------------------------------------
    private EntityTypeImpl buildEntityType(Vertex v, Optional<EntityType> type){
        return new EntityTypeImpl(graknGraph, v, type);
    }

    // ------------------------------------------- Building Entities
    EntityImpl buildEntity(Vertex v, EntityType type){
        return buildEntity(v, Optional.of(type));
    }
    private EntityImpl buildEntity(Vertex v, Optional<EntityType> type){
        return  new EntityImpl(graknGraph, v, type);
    }

    // ----------------------------------------- Building Rule Types  --------------------------------------------------
    private RuleTypeImpl buildRuleType(Vertex v, Optional<RuleType> type){
        return new RuleTypeImpl(graknGraph, v, type);
    }

    // -------------------------------------------- Building Rules
    RuleImpl buildRule(Vertex v, RuleType type, Pattern lhs, Pattern rhs){
        if(lhs == null)
            throw new InvalidConceptValueException(ErrorMessage.NULL_VALUE.getMessage(Schema.ConceptProperty.RULE_LHS.name()));

        if(rhs == null)
            throw new InvalidConceptValueException(ErrorMessage.NULL_VALUE.getMessage(Schema.ConceptProperty.RULE_RHS.name()));

        return buildRule(v, Optional.of(type), Optional.of(lhs), Optional.of(rhs));
    }
    private RuleImpl buildRule(Vertex v, Optional<RuleType> type, Optional<Pattern> lhs, Optional<Pattern> rhs){
        return new RuleImpl(graknGraph, v, type, lhs, rhs);
    }

    // ------------------------------------------ Building Roles  Types ------------------------------------------------
    RoleTypeImpl buildRoleType(Vertex v, RoleType type, Boolean isImplicit){
        return buildRoleType(v, Optional.of(type), Optional.of(isImplicit));
    }
    private RoleTypeImpl buildRoleType(Vertex v, Optional<RoleType> type, Optional<Boolean> isImplicit){
        return new RoleTypeImpl(graknGraph, v, type, isImplicit);
    }

    /**
     *
     * @param v A vertex of an unknown type
     * @return A concept built to the correct type
     */
    <X extends Concept> X buildConcept(Vertex v){
        Schema.BaseType type;
        try {
            type = Schema.BaseType.valueOf(v.label());
        } catch (IllegalArgumentException e){
            LOG.warn("Found vertex [" + v + "] which has an invalid base type [" + v.label() + "] ignoring . . . ");
            return null;
        }

        ConceptImpl concept = null;
        switch (type){
            case RELATION:
                concept = buildRelation(v, Optional.empty());
                break;
            case CASTING:
                concept = buildCasting(v, Optional.empty());
                break;
            case TYPE:
                concept = buildType(v, Optional.empty(), Optional.empty());
                break;
            case ROLE_TYPE:
                concept = buildRoleType(v, Optional.empty(), Optional.empty());
                break;
            case RELATION_TYPE:
                concept = buildRelationType(v, Optional.empty(), Optional.empty());
                break;
            case ENTITY:
                concept = buildEntity(v, Optional.empty());
                break;
            case ENTITY_TYPE:
                concept = buildEntityType(v, Optional.empty());
                break;
            case RESOURCE_TYPE:
                concept = buildResourceType(v, Optional.empty(), Optional.empty(), Optional.empty());
                break;
            case RESOURCE:
                concept = buildResource(v, Optional.empty(), Optional.empty());
                break;
            case RULE:
                concept = buildRule(v, Optional.empty(), Optional.empty(), Optional.empty());
                break;
            case RULE_TYPE:
                concept = buildRuleType(v, Optional.empty());
                break;
        }
        //noinspection unchecked
        return (X) concept;
    }

    TypeImpl buildSpecificType(Vertex vertex, Type type){
        Schema.BaseType baseType = Schema.BaseType.valueOf(vertex.label());
        TypeImpl conceptType;
        switch (baseType){
            case ROLE_TYPE:
                conceptType = buildRoleType(vertex, Optional.of(type.asRoleType()), Optional.empty());
                break;
            case RELATION_TYPE:
                conceptType = buildRelationType(vertex, Optional.of(type.asRelationType()), Optional.empty());
                break;
            case RESOURCE_TYPE:
                conceptType = buildResourceType(vertex, Optional.of(type.asResourceType()), Optional.empty(), Optional.empty());
                break;
            case RULE_TYPE:
                conceptType = buildRuleType(vertex, Optional.of(type.asRuleType()));
                break;
            case ENTITY_TYPE:
                conceptType = buildEntityType(vertex, Optional.of(type.asEntityType()));
                break;
            default:
                conceptType = buildType(vertex, Optional.of(type), Optional.empty());
        }
        return conceptType;
    }

    public EdgeImpl buildEdge(org.apache.tinkerpop.gremlin.structure.Edge edge, AbstractGraknGraph graknGraph){
        return new EdgeImpl(edge, graknGraph);
    }
}
