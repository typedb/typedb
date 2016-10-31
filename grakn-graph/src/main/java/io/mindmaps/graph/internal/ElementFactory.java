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

package io.mindmaps.graph.internal;

import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.RuleType;
import io.mindmaps.concept.Type;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal factory to produce different types of concepts
 */
final class ElementFactory {
    protected final Logger LOG = LoggerFactory.getLogger(ElementFactory.class);
    private final AbstractMindmapsGraph mindmapsGraph;

    public ElementFactory(AbstractMindmapsGraph mindmapsGraph){
        this.mindmapsGraph = mindmapsGraph;
    }

    public RelationImpl buildRelation(Vertex v, RelationType type){
        return new RelationImpl(v, type, mindmapsGraph);
    }

    public CastingImpl buildCasting(Vertex v, RoleType type){
        return new CastingImpl(v, type, mindmapsGraph);
    }

    public TypeImpl buildConceptType(Vertex v, Type type){
        return  new TypeImpl(v, type, mindmapsGraph);
    }

    public RuleTypeImpl buildRuleType(Vertex v, Type type){
        return  new RuleTypeImpl(v, type, mindmapsGraph);
    }

    public RoleTypeImpl buildRoleType(Vertex v, Type type){
        return new RoleTypeImpl(v, type, mindmapsGraph);
    }

    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Type type){
        return new ResourceTypeImpl<>(v, type, mindmapsGraph);
    }
    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Type type, ResourceType.DataType<V> dataType, boolean isUnique){
        return new ResourceTypeImpl<>(v, type, mindmapsGraph, dataType, isUnique);
    }

    public RelationTypeImpl buildRelationType(Vertex v, Type type){
        return  new RelationTypeImpl(v, type, mindmapsGraph);
    }

    public EntityTypeImpl buildEntityType(Vertex v, Type type){
        return  new EntityTypeImpl(v, type, mindmapsGraph);
    }

    public EntityImpl buildEntity(Vertex v, EntityType type){
        return  new EntityImpl(v, type, mindmapsGraph);
    }

    public <V> ResourceImpl <V> buildResource(Vertex v, ResourceType<V> type){
        return new ResourceImpl<>(v, type, mindmapsGraph);
    }

    public <V> ResourceImpl <V> buildResource(Vertex v, ResourceType<V> type, V value){
        return new ResourceImpl<>(v, type, mindmapsGraph, value);
    }

    public RuleImpl buildRule(Vertex v, RuleType type){
        return buildRule(v, type, v.value(Schema.ConceptProperty.RULE_LHS.name()), v.value(Schema.ConceptProperty.RULE_RHS.name()));
    }
    public RuleImpl buildRule(Vertex v, RuleType type, String lhs, String rhs){
        return  new RuleImpl(v, type, mindmapsGraph, lhs, rhs);
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

    public EdgeImpl buildEdge(org.apache.tinkerpop.gremlin.structure.Edge edge, AbstractMindmapsGraph mindmapsGraph){
        return new EdgeImpl(edge, mindmapsGraph);
    }
}
