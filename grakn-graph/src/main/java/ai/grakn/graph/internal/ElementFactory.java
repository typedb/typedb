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
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal factory to produce different types of concepts
 */
final class ElementFactory {
    private final Logger LOG = LoggerFactory.getLogger(ElementFactory.class);
    private final AbstractGraknGraph graknGraph;

    ElementFactory(AbstractGraknGraph graknGraph){
        this.graknGraph = graknGraph;
    }

    RelationImpl buildRelation(Vertex v, RelationType type){
        return new RelationImpl(v, type, graknGraph);
    }

    CastingImpl buildCasting(Vertex v, RoleType type){
        return new CastingImpl(v, type, graknGraph);
    }

    TypeImpl buildConceptType(Vertex v, Type type){
        return  new TypeImpl(v, type, graknGraph);
    }

    RuleTypeImpl buildRuleType(Vertex v, Type type){
        return  new RuleTypeImpl(v, type, graknGraph);
    }

    RoleTypeImpl buildRoleTypeImplicit(Vertex v, Type type){
        return new RoleTypeImpl(v, type, true, graknGraph);
    }
    RoleTypeImpl buildRoleType(Vertex v, Type type){
        return new RoleTypeImpl(v, type, graknGraph);
    }

    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Type type){
        return new ResourceTypeImpl<>(v, type, graknGraph);
    }
    <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Type type, ResourceType.DataType<V> dataType, boolean isUnique){
        return new ResourceTypeImpl<>(v, type, graknGraph, dataType, isUnique);
    }

    RelationTypeImpl buildRelationTypeImplicit(Vertex v, Type type){
        return  new RelationTypeImpl(v, type, true, graknGraph);
    }
    public RelationTypeImpl buildRelationType(Vertex v, Type type){
        return  new RelationTypeImpl(v, type, graknGraph);
    }

    public EntityTypeImpl buildEntityType(Vertex v, Type type){
        return  new EntityTypeImpl(v, type, graknGraph);
    }

    EntityImpl buildEntity(Vertex v, EntityType type){
        return  new EntityImpl(v, type, graknGraph);
    }

    private <V> ResourceImpl <V> buildResource(Vertex v, ResourceType<V> type){
        return new ResourceImpl<>(v, type, graknGraph);
    }

    <V> ResourceImpl <V> buildResource(Vertex v, ResourceType<V> type, V value){
        return new ResourceImpl<>(v, type, graknGraph, value);
    }

    private RuleImpl buildRule(Vertex v, RuleType type){
        Pattern lhs = graknGraph.graql().parsePattern(v.value(Schema.ConceptProperty.RULE_LHS.name()));
        Pattern rhs = graknGraph.graql().parsePattern(v.value(Schema.ConceptProperty.RULE_RHS.name()));
        return buildRule(v, type, lhs, rhs);
    }
    RuleImpl buildRule(Vertex v, RuleType type, Pattern lhs, Pattern rhs){
        return  new RuleImpl(v, type, graknGraph, lhs, rhs);
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
        //All these types are null because at this stage the concept has been defined so we don't need to know the type.
        switch (type){
            case RELATION:
                concept = buildRelation(v, null);
                break;
            case CASTING:
                concept = buildCasting(v, null);
                break;
            case TYPE:
                concept = buildConceptType(v, null);
                break;
            case ROLE_TYPE:
                concept = buildRoleType(v, null);
                break;
            case RELATION_TYPE:
                concept = buildRelationType(v, null);
                break;
            case ENTITY:
                concept = buildEntity(v, null);
                break;
            case ENTITY_TYPE:
                concept = buildEntityType(v, null);
                break;
            case RESOURCE_TYPE:
                concept = buildResourceType(v, null);
                break;
            case RESOURCE:
                concept = buildResource(v, null);
                break;
            case RULE:
                concept = buildRule(v, null);
                break;
            case RULE_TYPE:
                concept = buildRuleType(v, null);
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
                conceptType = buildRoleType(vertex, type);
                break;
            case RELATION_TYPE:
                conceptType = buildRelationType(vertex, type);
                break;
            case RESOURCE_TYPE:
                conceptType = buildResourceType(vertex, type);
                break;
            case RULE_TYPE:
                conceptType = buildRuleType(vertex, type);
                break;
            case ENTITY_TYPE:
                conceptType = buildEntityType(vertex, type);
                break;
            default:
                conceptType = buildConceptType(vertex, type);
        }
        return conceptType;
    }

    EdgeImpl buildEdge(org.apache.tinkerpop.gremlin.structure.Edge edge, AbstractGraknGraph graknGraph){
        return new EdgeImpl(edge, graknGraph);
    }
}
