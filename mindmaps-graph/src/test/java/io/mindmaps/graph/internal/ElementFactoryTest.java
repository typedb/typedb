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

import io.mindmaps.util.Schema;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.RuleType;
import io.mindmaps.concept.Type;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ElementFactoryTest {
    private AbstractMindmapsGraph mindmapsGraph;

    @Before
    public void buildGraphAccessManager(){
        mindmapsGraph = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();
    }

    @Test
    public void testBuildAssertion() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RELATION.name());
        RelationImpl relation = mindmapsGraph.getElementFactory().buildRelation(vertex);
        assertThat(relation, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        relation = mindmapsGraph.getElementFactory().buildRelation(concept);

        assertEquals(Schema.BaseType.RELATION.name(), relation.getBaseType());
    }

    @Test
    public void testBuildCasting() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        CastingImpl casting = mindmapsGraph.getElementFactory().buildCasting(vertex);
        assertThat(casting, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        casting = mindmapsGraph.getElementFactory().buildCasting(concept);
        assertEquals(concept, casting);

        assertEquals(Schema.BaseType.CASTING.name(), casting.getBaseType());
    }

    @Test
    public void testType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.TYPE.name());
        TypeImpl conceptType = mindmapsGraph.getElementFactory().buildConceptType(vertex);
        assertThat(conceptType, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        conceptType = mindmapsGraph.getElementFactory().buildConceptType(concept);
        assertEquals(concept, conceptType);

        assertEquals(Schema.BaseType.TYPE.name(), conceptType.getBaseType());
    }

    @Test
    public void testBuildRoleType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.ROLE_TYPE.name());
        RoleTypeImpl roleType = mindmapsGraph.getElementFactory().buildRoleType(vertex);
        assertThat(roleType, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        roleType = mindmapsGraph.getElementFactory().buildRoleType(concept);
        assertEquals(concept, roleType);

        assertEquals(Schema.BaseType.ROLE_TYPE.name(), roleType.getBaseType());
    }

    @Test
    public void testBuildRelationType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RELATION_TYPE.name());
        RelationTypeImpl relationType = mindmapsGraph.getElementFactory().buildRelationType(vertex);
        assertThat(relationType, instanceOf(ConceptImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        relationType = mindmapsGraph.getElementFactory().buildRelationType(concept);
        assertEquals(concept, relationType);

        assertEquals(Schema.BaseType.RELATION_TYPE.name(), relationType.getBaseType());
    }

    @Test
    public void testBuildConceptInstance() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.ENTITY.name());
        InstanceImpl instance = mindmapsGraph.getElementFactory().buildEntity(vertex);
        assertThat(instance, instanceOf(InstanceImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        instance = mindmapsGraph.getElementFactory().buildEntity(concept);
        assertEquals(concept, instance);

        assertEquals(Schema.BaseType.ENTITY.name(), instance.getBaseType());
    }

    @Test
    public void testBuildResourceType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE_TYPE.name());
        ResourceTypeImpl resource = mindmapsGraph.getElementFactory().buildResourceType(vertex);
        assertThat(resource, instanceOf(ResourceTypeImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        resource = mindmapsGraph.getElementFactory().buildResourceType(concept, ResourceType.DataType.STRING);
        assertEquals(concept, resource);

        assertEquals(Schema.BaseType.RESOURCE_TYPE.name(), resource.getBaseType());
    }

    @Test
    public void testBuildRuleType() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RULE_TYPE.name());
        RuleType rule = mindmapsGraph.getElementFactory().buildRuleType(vertex);
        assertThat(rule, instanceOf(TypeImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        rule = mindmapsGraph.getElementFactory().buildRuleType(concept);
        assertEquals(concept, rule);

        assertEquals(Schema.BaseType.RULE_TYPE.name(), ((ConceptImpl) rule).getBaseType());
    }

    @Test
    public void testBuildRule() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RULE.name());
        vertex.property(Schema.ConceptProperty.RULE_LHS.name(), "lhs");
        vertex.property(Schema.ConceptProperty.RULE_RHS.name(), "rhs");

        RuleImpl rule = mindmapsGraph.getElementFactory().buildRule(vertex);
        assertThat(rule, instanceOf(RuleImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        rule = mindmapsGraph.getElementFactory().buildRule((ConceptImpl) concept);
        assertEquals(concept, rule);

        assertEquals(Schema.BaseType.RULE.name(), rule.getBaseType());
    }

    @Test
    public void testBuildResource() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        ResourceImpl resource = mindmapsGraph.getElementFactory().buildResource(vertex);
        assertThat(resource, instanceOf(ResourceImpl.class));

        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        resource = mindmapsGraph.getElementFactory().buildResource(concept);
        assertEquals(concept, resource);

        assertEquals(Schema.BaseType.RESOURCE.name(), resource.getBaseType());
    }

    @Test
    public void testEquality() throws Exception {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.TYPE.name());
        Concept concept = mindmapsGraph.getElementFactory().buildUnknownConcept(vertex);
        Type type = mindmapsGraph.getElementFactory().buildConceptType(vertex);
        assertEquals(concept, type);
    }

    @Test
    public void testBuildUnknownConcept() throws Exception{
        Vertex v2 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RELATION.name());
        Vertex v4 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        Vertex v5 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.TYPE.name());
        Vertex v6 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.ROLE_TYPE.name());
        Vertex v7 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RELATION_TYPE.name());
        Vertex v8 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.ENTITY.name());
        Vertex v9 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE_TYPE.name());
        Vertex v10 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        Vertex v11 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RULE_TYPE.name());
        Vertex v12 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RULE.name());
        v12.property(Schema.ConceptProperty.RULE_LHS.name(), "lhs");
        v12.property(Schema.ConceptProperty.RULE_RHS.name(), "rhs");

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
        Vertex c1 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.TYPE.name());
        Vertex c2 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RELATION_TYPE.name());
        Vertex c3 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.ROLE_TYPE.name());
        Vertex c4 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE_TYPE.name());

        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c1), instanceOf(Type.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c2), instanceOf(RelationType.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c3), instanceOf(RoleType.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificConceptType(c4), instanceOf(ResourceType.class));
    }

    @Test
    public void testBuildSpecificConceptInstance(){
        Vertex conceptInstance = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.ENTITY.name());
        Vertex assertion = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RELATION.name());
        Vertex rule = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RULE.name());
        rule.property(Schema.ConceptProperty.RULE_LHS.name(), "lhs");
        rule.property(Schema.ConceptProperty.RULE_RHS.name(), "rhs");

        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(conceptInstance), instanceOf(Instance.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(assertion), instanceOf(Relation.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(rule), instanceOf(Rule.class));
    }

    @Test
    public void testBuildSpecificConceptInstance2(){
        Vertex c1 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.ENTITY.name());
        Vertex c2 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RELATION.name());
        Vertex c3 = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());

        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(c1), instanceOf(Instance.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(c2), instanceOf(Relation.class));
        assertThat(mindmapsGraph.getElementFactory().buildSpecificInstance(c3), instanceOf(Resource.class));
    }
}