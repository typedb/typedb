package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Concept;
import org.apache.tinkerpop.gremlin.structure.Vertex;

final class ElementFactory {
    private final MindmapsTransactionImpl mindmapsGraph;

    public ElementFactory(MindmapsTransactionImpl mindmapsGraph){
        this.mindmapsGraph = mindmapsGraph;
    }

    public RelationImpl buildRelation(Vertex v){
        return new RelationImpl(v, mindmapsGraph);
    }
    public RelationImpl buildRelation(Concept c){
        return buildRelation(((ConceptImpl) c).getVertex());
    }

    public CastingImpl buildCasting(Vertex v){
        return new CastingImpl(v, mindmapsGraph);
    }
    public CastingImpl buildCasting(Concept c){
        return buildCasting(((ConceptImpl) c).getVertex());
    }

    public TypeImpl buildConceptType(Vertex v){
        return  new TypeImpl(v, mindmapsGraph);
    }
    public TypeImpl buildConceptType(Concept c){
        return buildConceptType(((ConceptImpl) c).getVertex());
    }

    public RuleTypeImpl buildRuleType(Vertex v){
        return  new RuleTypeImpl(v, mindmapsGraph);
    }
    public RuleTypeImpl buildRuleType(Concept c){
        return buildRuleType(((ConceptImpl) c).getVertex());
    }

    public RoleTypeImpl buildRoleType(Vertex v){
        return new RoleTypeImpl(v, mindmapsGraph);
    }
    public RoleTypeImpl buildRoleType(Concept c){
        return buildRoleType(((ConceptImpl) c).getVertex());
    }

    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v){
        return new ResourceTypeImpl<>(v, mindmapsGraph);
    }
    public <V> ResourceTypeImpl<V> buildResourceType(Vertex v, Data<V> type){
        return new ResourceTypeImpl<>(v, mindmapsGraph, type);
    }
    public <V> ResourceTypeImpl<V> buildResourceType(Concept c, Data<V> type){
        return buildResourceType(((ConceptImpl) c).getVertex(), type);
    }

    public RelationTypeImpl buildRelationType(Vertex v){
        return  new RelationTypeImpl(v, mindmapsGraph);
    }
    public RelationTypeImpl buildRelationType(Concept c){
        return buildRelationType(((ConceptImpl) c).getVertex());
    }

    public EntityTypeImpl buildEntityType(Vertex v){
        return  new EntityTypeImpl(v, mindmapsGraph);
    }
    public EntityTypeImpl buildEntityType(Concept c){
        return buildEntityType(((ConceptImpl) c).getVertex());
    }

    public EntityImpl buildEntity(Vertex v){
        return  new EntityImpl(v, mindmapsGraph);
    }
    public EntityImpl buildEntity(Concept c){
        return buildEntity(((ConceptImpl) c).getVertex());
    }

    public <V> ResourceImpl <V> buildResource(Vertex v){
        return  new ResourceImpl<>(v, mindmapsGraph);
    }
    public <V> ResourceImpl <V> buildResource(Concept c){
        return  buildResource(((ConceptImpl) c).getVertex());
    }

    public RuleImpl buildRule(Vertex v){
        return  new RuleImpl(v, mindmapsGraph);
    }
    public RuleImpl buildRule(Concept c){
        return  buildRule(((ConceptImpl) c).getVertex());
    }

    public ConceptImpl buildUnknownConcept(Concept concept){
        return  buildUnknownConcept(((ConceptImpl) concept).getVertex());
    }
    public ConceptImpl buildUnknownConcept(Vertex v){
        DataType.BaseType type = DataType.BaseType.valueOf(v.label());
        ConceptImpl concept = null;
        switch (type){
            case RELATION:
                concept = buildRelation(v);
                break;
            case CASTING:
                concept = buildCasting(v);
                break;
            case TYPE:
                concept = buildConceptType(v);
                break;
            case ROLE_TYPE:
                concept = buildRoleType(v);
                break;
            case RELATION_TYPE:
                concept = buildRelationType(v);
                break;
            case ENTITY:
                concept = buildEntity(v);
                break;
            case ENTITY_TYPE:
                concept = buildEntityType(v);
                break;
            case RESOURCE_TYPE:
                concept = buildResourceType(v);
                break;
            case RESOURCE:
                concept = buildResource(v);
                break;
            case RULE:
                concept = buildRule(v);
                break;
            case RULE_TYPE:
                concept = buildRuleType(v);
                break;
        }
        return concept;
    }

    public TypeImpl buildSpecificConceptType(Concept concept){
        return buildSpecificConceptType(((ConceptImpl) concept).getVertex());
    }

    public TypeImpl buildSpecificConceptType(Vertex vertex){
        DataType.BaseType type = DataType.BaseType.valueOf(vertex.label());
        TypeImpl conceptType;
        switch (type){
            case ROLE_TYPE:
                conceptType = buildRoleType(vertex);
                break;
            case RELATION_TYPE:
                conceptType = buildRelationType(vertex);
                break;
            case RESOURCE_TYPE:
                conceptType = buildResourceType(vertex);
                break;
            case RULE_TYPE:
                conceptType = buildRuleType(vertex);
                break;
            case ENTITY_TYPE:
                conceptType = buildEntityType(vertex);
                break;
            default:
                conceptType = buildConceptType(vertex);
        }
        return conceptType;
    }


    public InstanceImpl buildSpecificInstance(Concept concept){
        return  buildSpecificInstance(((ConceptImpl) concept).getVertex());
    }

    public InstanceImpl buildSpecificInstance(Vertex vertex){
        DataType.BaseType type = DataType.BaseType.valueOf(vertex.label());
        InstanceImpl conceptInstance;
        switch (type){
            case RELATION:
                conceptInstance = buildRelation(vertex);
                break;
            case RESOURCE:
                conceptInstance = buildResource(vertex);
                break;
            case RULE:
                conceptInstance = buildRule(vertex);
                break;
            default:
                conceptInstance = buildEntity(vertex);
        }
        return conceptInstance;
    }

    public EdgeImpl buildEdge(org.apache.tinkerpop.gremlin.structure.Edge edge, MindmapsTransactionImpl mindmapsGraph){
        return new EdgeImpl(edge, mindmapsGraph);
    }
}
