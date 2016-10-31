/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.graph.internal;

import io.grakn.concept.EntityType;
import io.grakn.concept.RelationType;
import io.grakn.concept.ResourceType;
import io.grakn.concept.RoleType;
import io.grakn.concept.RuleType;
import io.grakn.concept.Type;
import io.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal factory to produce different types of concepts
 */
final class ElementFactory {
    protected final Logger LOG = LoggerFactory.getLogger(ElementFactory.class);
    private final AbstractGraknGraph graknGraph;

    public ElementFactory(AbstractGraknGraph graknGraph){
        this.graknGraph = graknGraph;
    }

    public RelationImpl buildRelation(Vertex v, RelationType type){
        return new RelationImpl(v, type, graknGraph);
    }

    public CastingImpl buildCasting(Vertex v, RoleType type){
        return new CastingImpl(v, type, graknGraph);
    }

    public TypeImpl buildConceptType(Vertex v, Type type){
        return  new TypeImpl(v, type, graknGraph);
    }

    public RuleTypeImpl buildRuleType(Vertex v, Type type){
        return  new RuleTypeImpl(v, type, graknGraph);
    }

    public RoleTypeImpl buildRoleType(Vertex v, Type type){
        return new RoleTypeImpl(v, type, graknGraph);
    }

    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Type type){
        return new ResourceTypeImpl<>(v, type, graknGraph);
    }
    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Type type, ResourceType.DataType<V> dataType, boolean isUnique){
        return new ResourceTypeImpl<>(v, type, graknGraph, dataType, isUnique);
    }

    public RelationTypeImpl buildRelationType(Vertex v, Type type){
        return  new RelationTypeImpl(v, type, graknGraph);
    }

    public EntityTypeImpl buildEntityType(Vertex v, Type type){
        return  new EntityTypeImpl(v, type, graknGraph);
    }

    public EntityImpl buildEntity(Vertex v, EntityType type){
        return  new EntityImpl(v, type, graknGraph);
    }

    public <V> ResourceImpl <V> buildResource(Vertex v, ResourceType<V> type){
        return new ResourceImpl<>(v, type, graknGraph);
    }

    public <V> ResourceImpl <V> buildResource(Vertex v, ResourceType<V> type, V value){
        return new ResourceImpl<>(v, type, graknGraph, value);
    }

    public RuleImpl buildRule(Vertex v, RuleType type){
        return buildRule(v, type, v.value(Schema.ConceptProperty.RULE_LHS.name()), v.value(Schema.ConceptProperty.RULE_RHS.name()));
    }
    public RuleImpl buildRule(Vertex v, RuleType type, String lhs, String rhs){
        return  new RuleImpl(v, type, graknGraph, lhs, rhs);
    }

    /**
     *
     * @param v A vertex of an unknown type
     * @return A concept built to the correct type
     */
    public ConceptImpl buildUnknownConcept(Vertex v){
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
        return concept;
    }

    public TypeImpl buildSpecificConceptType(Vertex vertex, Type type){
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

    public EdgeImpl buildEdge(org.apache.tinkerpop.gremlin.structure.Edge edge, AbstractGraknGraph graknGraph){
        return new EdgeImpl(edge, graknGraph);
    }
}
