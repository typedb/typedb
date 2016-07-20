package io.mindmaps.core.implementation;

import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ElementFactoryTest {
    private MindmapsTransactionImpl mindmapsGraph;

    @Before
    public void buildGraphAccessManager(){
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
    }

    @Test
    public void testBuildAssertion() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RELATION.name());
        RelationImpl relation = mindmapsGraph.getElementFactory().buildRelation(vertex);
        assertThat(relation, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        relation = mindmapsGraph.getElementFactory().buildRelation(concept);

        assertEquals(DataType.BaseType.RELATION.name(), relation.getBaseType());
    }

    @Test
    public void testBuildCasting() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.CASTING.name());
        CastingImpl casting = mindmapsGraph.getElementFactory().buildCasting(vertex);
        assertThat(casting, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        casting = mindmapsGraph.getElementFactory().buildCasting(concept);
        assertEquals(concept, casting);

        assertEquals(DataType.BaseType.CASTING.name(), casting.getBaseType());
    }

    @Test
    public void testType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.TYPE.name());
        TypeImpl conceptType = mindmapsGraph.getElementFactory().buildConceptType(vertex);
        assertThat(conceptType, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        conceptType = mindmapsGraph.getElementFactory().buildConceptType(concept);
        assertEquals(concept, conceptType);

        assertEquals(DataType.BaseType.TYPE.name(), conceptType.getBaseType());
    }

    @Test
    public void testBuildRoleType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.ROLE_TYPE.name());
        RoleTypeImpl roleType = mindmapsGraph.getElementFactory().buildRoleType(vertex);
        assertThat(roleType, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        roleType = mindmapsGraph.getElementFactory().buildRoleType(concept);
        assertEquals(concept, roleType);

        assertEquals(DataType.BaseType.ROLE_TYPE.name(), roleType.getBaseType());
    }

    @Test
    public void testBuildRelationType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RELATION_TYPE.name());
        RelationTypeImpl relationType = mindmapsGraph.getElementFactory().buildRelationType(vertex);
        assertThat(relationType, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        relationType = mindmapsGraph.getElementFactory().buildRelationType(concept);
        assertEquals(concept, relationType);

        assertEquals(DataType.BaseType.RELATION_TYPE.name(), relationType.getBaseType());
    }

    @Test
    public void testBuildConceptInstance() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.ENTITY.name());
        InstanceImpl instance = mindmapsGraph.getElementFactory().buildEntity(vertex);
        assertThat(instance, instanceOf(InstanceImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        instance = mindmapsGraph.getElementFactory().buildEntity(concept);
        assertEquals(concept, instance);

        assertEquals(DataType.BaseType.ENTITY.name(), instance.getBaseType());
    }

    @Test
    public void testBuildResourceType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RESOURCE_TYPE.name());
        ResourceTypeImpl resource = mindmapsGraph.getElementFactory().buildResourceType(vertex);
        assertThat(resource, instanceOf(ResourceTypeImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        resource = mindmapsGraph.getElementFactory().buildResourceType(concept, Data.STRING);
        assertEquals(concept, resource);

        assertEquals(DataType.BaseType.RESOURCE_TYPE.name(), resource.getBaseType());
    }

    @Test
    public void testBuildRuleType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RULE_TYPE.name());
        RuleType rule = mindmapsGraph.getElementFactory().buildRuleType(vertex);
        assertThat(rule, instanceOf(TypeImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        rule = mindmapsGraph.getElementFactory().buildRuleType(concept);
        assertEquals(concept, rule);

        assertEquals(DataType.BaseType.RULE_TYPE.name(), ((ConceptImpl) rule).getBaseType());
    }

    @Test
    public void testBuildRule() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RULE.name());
        RuleImpl rule = mindmapsGraph.getElementFactory().buildRule(vertex);
        assertThat(rule, instanceOf(RuleImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        rule = mindmapsGraph.getElementFactory().buildRule(concept);
        assertEquals(concept, rule);

        assertEquals(DataType.BaseType.RULE.name(), rule.getBaseType());
    }

    @Test
    public void testBuildResource() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RESOURCE.name());
        ResourceImpl resource = mindmapsGraph.getElementFactory().buildResource(vertex);
        assertThat(resource, instanceOf(ResourceImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        resource = mindmapsGraph.getElementFactory().buildResource(concept);
        assertEquals(concept, resource);

        assertEquals(DataType.BaseType.RESOURCE.name(), resource.getBaseType());
    }

    @Test
    public void testEquality() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.TYPE.name());
        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        Type type = mindmapsGraph.getElementFactory().buildConceptType(vertex);
        assertEquals(concept, type);
    }

    @Test
    public void testBuildUnknownConcept() throws Exception{
        Vertex v2 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RELATION.name());
        Vertex v4 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.CASTING.name());
        Vertex v5 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.TYPE.name());
        Vertex v6 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.ROLE_TYPE.name());
        Vertex v7 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RELATION_TYPE.name());
        Vertex v8 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.ENTITY.name());
        Vertex v9 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RESOURCE_TYPE.name());
        Vertex v10 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RESOURCE.name());
        Vertex v11 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RULE_TYPE.name());
        Vertex v12 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RULE.name());

        Concept assertion = mindmapsGraph.getElementFactory().buildUnknownConcept(v2);
        Concept casting = mindmapsGraph.getElementFactory().buildUnknownConcept(v4);
        Concept conceptType = mindmapsGraph.getElementFactory().buildUnknownConcept(v5);
        Concept roleType = mindmapsGraph.getElementFactory().buildUnknownConcept(v6);
        Concept relationType = mindmapsGraph.getElementFactory().buildUnknownConcept(v7);
        Concept conceptInstance = mindmapsGraph.getElementFactory().buildUnknownConcept(v8);
        Concept resourceType = mindmapsGraph.getElementFactory().buildUnknownConcept(v9);
        Concept resource = mindmapsGraph.getElementFactory().buildUnknownConcept(v10);
        Concept ruleType = mindmapsGraph.getElementFactory().buildUnknownConcept(v11);
        Concept rule = mindmapsGraph.getElementFactory().buildUnknownConcept(v12);

        assertThat(assertion, instanceOf(Relation.class));
        assertThat(casting, instanceOf(CastingImpl.class));
        assertThat(conceptType, instanceOf(Type.class));
        assertThat(roleType, instanceOf(RoleType.class));
        assertThat(relationType, instanceOf(RelationType.class));
        assertThat(conceptInstance, instanceOf(Instance.class));
        assertThat(resourceType, instanceOf(ResourceType.class));
        assertThat(resource, instanceOf(Resource.class));
        assertThat(ruleType, instanceOf(RuleType.class));
        assertThat(rule, instanceOf(Rule.class));
    }

    @Test
    public void testBuildSpecificConceptType() throws Exception {
        Vertex c1 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.TYPE.name());
        Vertex c2 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RELATION_TYPE.name());
        Vertex c3 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.ROLE_TYPE.name());
        Vertex c4 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RESOURCE_TYPE.name());

        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c1), instanceOf(Type.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c2), instanceOf(RelationType.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c3), instanceOf(RoleType.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c4), instanceOf(ResourceType.class));
    }

    @Test
    public void testBuildSpecificConceptInstance(){
        Vertex conceptInstance = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.ENTITY.name());
        Vertex assertion = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RELATION.name());
        Vertex rule = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RULE.name());

        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(conceptInstance), instanceOf(Instance.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(assertion), instanceOf(Relation.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(rule), instanceOf(Rule.class));
    }

    @Test
    public void testBuildSpecificConceptInstance2(){
        Vertex c1 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.ENTITY.name());
        Vertex c2 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RELATION.name());
        Vertex c3 = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.RESOURCE.name());

        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(c1), instanceOf(Instance.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(c2), instanceOf(Relation.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(c3), instanceOf(Resource.class));
    }
}