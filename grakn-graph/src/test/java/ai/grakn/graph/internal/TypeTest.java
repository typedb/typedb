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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.ConceptException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class TypeTest extends GraphTestBase{

    @Before
    public void buildGraph(){
        EntityType top = graknGraph.putEntityType("top");
        EntityType middle1 = graknGraph.putEntityType("mid1");
        EntityType middle2 = graknGraph.putEntityType("mid2");
        EntityType middle3 = graknGraph.putEntityType("mid3'");
        EntityType bottom = graknGraph.putEntityType("bottom");

        bottom.superType(middle1);
        middle1.superType(top);
        middle2.superType(top);
        middle3.superType(top);
    }

    @Test
    public void testItemName(){
        Type test = graknGraph.putEntityType("test");
        assertEquals("test", test.getName());
    }

    @Test
    public void testGetRoleTypeAsConceptType(){
        RoleType test1 = graknGraph.putRoleType("test");
        Type test2 = graknGraph.getEntityType("test");
        assertNull(test2);
    }

    @Test
    public void testGetPlayedRole() throws Exception{
        RoleType monster = graknGraph.putRoleType("monster");
        RoleType animal = graknGraph.putRoleType("animal");
        RoleType monsterSub = graknGraph.putRoleType("monsterSub");

        EntityType creature = graknGraph.putEntityType("creature");
        EntityType creatureSub = graknGraph.putEntityType("creatureSub").superType(creature);

        assertEquals(0, creature.playsRoles().size());
        assertEquals(0, creatureSub.playsRoles().size());

        creature.playsRole(monster);
        creature.playsRole(animal);
        monsterSub.superType(monster);

        creatureSub.playsRole(monsterSub);

        assertEquals(2, creature.playsRoles().size());
        assertTrue(creature.playsRoles().contains(monster));
        assertTrue(creature.playsRoles().contains(animal));

        assertEquals(1, creatureSub.playsRoles().size());
        assertTrue(creatureSub.playsRoles().contains(monsterSub));
    }

    @Test
    public void testGetSubHierarchySuperSet() throws Exception{
        TypeImpl c1 = (TypeImpl) graknGraph.putEntityType("c1");
        TypeImpl c2 = (TypeImpl) graknGraph.putEntityType("c2");
        TypeImpl c3 = (TypeImpl) graknGraph.putEntityType("c3'");
        TypeImpl c4 = (TypeImpl) graknGraph.putEntityType("c4");

        assertTrue(c1.getSubHierarchySuperSet().contains(c1));
        assertFalse(c1.getSubHierarchySuperSet().contains(c2));
        assertFalse(c1.getSubHierarchySuperSet().contains(c3));
        assertFalse(c1.getSubHierarchySuperSet().contains(c4));

        c1.superType(c2);
        assertTrue(c1.getSubHierarchySuperSet().contains(c1));
        assertTrue(c1.getSubHierarchySuperSet().contains(c2));
        assertFalse(c1.getSubHierarchySuperSet().contains(c3));
        assertFalse(c1.getSubHierarchySuperSet().contains(c4));

        c2.superType(c3);
        assertTrue(c1.getSubHierarchySuperSet().contains(c1));
        assertTrue(c1.getSubHierarchySuperSet().contains(c2));
        assertTrue(c1.getSubHierarchySuperSet().contains(c3));
        assertFalse(c1.getSubHierarchySuperSet().contains(c4));

        graknGraph.getTinkerPopGraph().traversal().V().
                hasId(c3.getId()).
                outE(Schema.EdgeLabel.ISA.getLabel()).next().remove();
        c3.superType(c4);
        boolean correctExceptionThrown = false;
        try{
            c4.superType(c2);
            c1.getSubHierarchySuperSet();
        } catch(RuntimeException e){
            correctExceptionThrown = e.getMessage().contains("loop");
        }
        assertTrue(correctExceptionThrown);

    }

    @Test
    public void testCannotSubClassMetaTypes(){
        RuleType metaType = graknGraph.getMetaRuleInference();
        RuleType superType = graknGraph.putRuleType("An Entity Type");

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(metaType.getName()))
        ));

        superType.superType(metaType);
    }

    @Test
    public void testGetSubChildrenSet(){
        EntityType parent = graknGraph.putEntityType("parent");
        EntityType child1 = graknGraph.putEntityType("c1");
        EntityType child2 = graknGraph.putEntityType("c2");
        EntityType child3 = graknGraph.putEntityType("c3");

        assertEquals(1, parent.subTypes().size());

        child1.superType(parent);
        child2.superType(parent);
        child3.superType(parent);

        assertEquals(4, parent.subTypes().size());
        assertTrue(parent.subTypes().contains(child3));
        assertTrue(parent.subTypes().contains(child2));
        assertTrue(parent.subTypes().contains(child1));
    }

    @Test
    public void testGetSubHierarchySubSet(){
        EntityType parent = graknGraph.putEntityType("p");
        EntityType superParent = graknGraph.putEntityType("sp");
        EntityType child1 = graknGraph.putEntityType("c1");
        EntityType child2 = graknGraph.putEntityType("c2");
        EntityType child3 = graknGraph.putEntityType("c3");
        EntityType child3a = graknGraph.putEntityType("3a");
        EntityType child3b = graknGraph.putEntityType("3b");
        EntityType child3b1 = graknGraph.putEntityType("3b1");
        EntityType child3b2 = graknGraph.putEntityType("3b2");
        EntityType child3b3 = graknGraph.putEntityType("3b3");

        assertEquals(1, ((TypeImpl) parent).subTypes().size());

        parent.superType(superParent);
        child1.superType(parent);
        child2.superType(parent);
        child3.superType(parent);
        child3a.superType(child3);
        child3b.superType(child3a);
        child3b1.superType(child3b);
        child3b2.superType(child3b);
        child3b3.superType(child3b);

        assertEquals(9, ((TypeImpl) parent).subTypes().size());
        assertTrue(((TypeImpl) parent).subTypes().contains(parent));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3));
        assertTrue(((TypeImpl) parent).subTypes().contains(child2));
        assertTrue(((TypeImpl) parent).subTypes().contains(child1));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3a));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b1));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b2));
        assertTrue(((TypeImpl) parent).subTypes().contains(child3b3));
        assertFalse(((TypeImpl) parent).subTypes().contains(superParent));

    }

    @Test
    public void testDuplicateConceptType(){
        Type movie = graknGraph.putEntityType("movie");
        Type moive2 = graknGraph.putEntityType("movie");
        assertEquals(movie, moive2);
    }

    @Test
    public void testSuperConceptType(){
        EntityType parent = graknGraph.putEntityType("p");
        EntityType superParent = graknGraph.putEntityType("sp");
        EntityType superParent2 = graknGraph.putEntityType("sp2");

        parent.superType(superParent);
        assertNotEquals(superParent2, parent.superType());
        assertEquals(superParent, parent.superType());

        parent.superType(superParent2);
        assertNotEquals(superParent, parent.superType());
        assertEquals(superParent2, parent.superType());
    }

    @Test
    public void allowsRoleType(){
        EntityTypeImpl conceptType = (EntityTypeImpl) graknGraph.putEntityType("ct");
        RoleType roleType1 = graknGraph.putRoleType("rt1'");
        RoleType roleType2 = graknGraph.putRoleType("rt2");

        conceptType.playsRole(roleType1).playsRole(roleType2);
        Set<RoleType> foundRoles = new HashSet<>();
        graknGraph.getTinkerPopGraph().traversal().V(conceptType.getBaseIdentifier()).
                out(Schema.EdgeLabel.PLAYS_ROLE.getLabel()).forEachRemaining(r -> foundRoles.add(graknGraph.getRoleType(r.value(Schema.ConceptProperty.NAME.name()))));

        assertEquals(2, foundRoles.size());
        assertTrue(foundRoles.contains(roleType1));
        assertTrue(foundRoles.contains(roleType2));
    }

    @Test
    public void checkSuperConceptTypeOverride(){
        EntityTypeImpl conceptType = (EntityTypeImpl) graknGraph.putEntityType("A Thing");
        EntityTypeImpl conceptType2 = (EntityTypeImpl) graknGraph.putEntityType("A Super Thing");
        assertNotNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.ISA));
        assertNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.SUB));
        conceptType.superType(conceptType2);
        assertNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.ISA));
        assertNotNull(conceptType.getOutgoingNeighbour(Schema.EdgeLabel.SUB));
    }

    @Test
    public void testRulesOfHypothesis(){
        Pattern lhs = graknGraph.graql().parsePattern("$x isa entity-type");
        Pattern rhs = graknGraph.graql().parsePattern("$x isa entity-type");
        Type type = graknGraph.putEntityType("A Concept Type");
        RuleType ruleType = graknGraph.putRuleType("A Rule Type");
        assertEquals(0, type.getRulesOfHypothesis().size());
        Rule rule1 = ruleType.addRule(lhs, rhs).addHypothesis(type);
        Rule rule2 = ruleType.addRule(lhs, rhs).addHypothesis(type);
        assertEquals(2, type.getRulesOfHypothesis().size());
        assertTrue(type.getRulesOfHypothesis().contains(rule1));
        assertTrue(type.getRulesOfHypothesis().contains(rule2));
    }

    @Test
    public void getRulesOfConclusion(){
        Pattern lhs = graknGraph.graql().parsePattern("$x isa entity-type");
        Pattern rhs = graknGraph.graql().parsePattern("$x isa entity-type");
        Type type = graknGraph.putEntityType("A Concept Type");
        RuleType ruleType = graknGraph.putRuleType("A Rule Type");
        assertEquals(0, type.getRulesOfConclusion().size());
        Rule rule1 = ruleType.addRule(lhs, rhs).addConclusion(type);
        Rule rule2 = ruleType.addRule(lhs, rhs).addConclusion(type);
        assertEquals(2, type.getRulesOfConclusion().size());
        assertTrue(type.getRulesOfConclusion().contains(rule1));
        assertTrue(type.getRulesOfConclusion().contains(rule2));
    }

    @Test
    public void testDeletePlaysRole(){
        EntityType type = graknGraph.putEntityType("A Concept Type");
        RoleType role1 = graknGraph.putRoleType("A Role 1");
        RoleType role2 = graknGraph.putRoleType("A Role 2");
        assertEquals(0, type.playsRoles().size());
        type.playsRole(role1).playsRole(role2);
        assertEquals(2, type.playsRoles().size());
        assertTrue(type.playsRoles().contains(role1));
        assertTrue(type.playsRoles().contains(role2));
        type.deletePlaysRole(role1);
        assertEquals(1, type.playsRoles().size());
        assertFalse(type.playsRoles().contains(role1));
        assertTrue(type.playsRoles().contains(role2));
    }

    @Test
    public void testDeleteConceptType(){
        EntityType toDelete = graknGraph.putEntityType("1");
        assertNotNull(graknGraph.getEntityType("1"));
        toDelete.delete();
        assertNull(graknGraph.getEntityType("1"));

        toDelete = graknGraph.putEntityType("2");
        Instance instance = toDelete.addEntity();

        boolean conceptExceptionThrown = false;
        try{
            toDelete.delete();
        } catch (ConceptException e){
            conceptExceptionThrown = true;
        }
        assertTrue(conceptExceptionThrown);
    }

    @Test
    public void testGetInstances(){
        EntityType entityType = graknGraph.putEntityType("Entity");
        RoleType actor = graknGraph.putRoleType("Actor");
        entityType.addEntity();
        EntityType production = graknGraph.putEntityType("Production");
        EntityType movie = graknGraph.putEntityType("Movie").superType(production);
        Instance musicVideo = production.addEntity();
        Instance godfather = movie.addEntity();

        Collection<? extends Concept> types = graknGraph.getMetaType().instances();
        Collection<? extends Concept> data = production.instances();

        assertEquals(11, types.size());
        assertEquals(2, data.size());

        assertTrue(types.contains(actor));
        assertTrue(types.contains(movie));
        assertTrue(types.contains(production));

        assertTrue(data.contains(godfather));
        assertTrue(data.contains(musicVideo));
    }

    @Test(expected=ConceptException.class)
    public void testCircularSub(){
        EntityType entityType = graknGraph.putEntityType("Entity");
        entityType.superType(entityType);
    }

    @Test(expected=ConceptException.class)
    public void testCircularSubLong(){
        EntityType entityType1 = graknGraph.putEntityType("Entity1");
        EntityType entityType2 = graknGraph.putEntityType("Entity2");
        EntityType entityType3 = graknGraph.putEntityType("Entity3");
        entityType1.superType(entityType2);
        entityType2.superType(entityType3);
        entityType3.superType(entityType1);
    }


    @Test
    public void testMetaTypeIsAbstractImmutable(){
        Type meta = graknGraph.getMetaRuleType();

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(meta.getName()))
        ));

        meta.setAbstract(true);
    }

    @Test
    public void testMetaTypePlaysRoleImmutable(){
        Type meta = graknGraph.getMetaRuleType();
        RoleType roleType = graknGraph.putRoleType("A Role");

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(meta.getName()))
        ));

        meta.playsRole(roleType);
    }

    @Test
    public void testHasResource(){
        String resourceTypeId = "Resource Type";
        EntityType entityType = graknGraph.putEntityType("Entity1");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        RelationType relationType = entityType.hasResource(resourceType);
        assertEquals(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId), relationType.getName());

        Set<String> roleNames = relationType.hasRoles().stream().map(Type::getName).collect(Collectors.toSet());
        assertEquals(2, roleNames.size());

        assertTrue(roleNames.contains(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId)));
        assertTrue(roleNames.contains(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId)));

        assertEquals(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId), entityType.playsRoles().iterator().next().getName());
        assertEquals(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId), resourceType.playsRoles().iterator().next().getName());

        //Check everything is implicit
        assertTrue(relationType.isImplicit());
        relationType.hasRoles().forEach(role -> assertTrue(role.isImplicit()));

        // Check that resource is not required
        EdgeImpl entityPlays = ((EntityTypeImpl) entityType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertFalse(entityPlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
        EdgeImpl resourcePlays = ((ResourceTypeImpl) resourceType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertFalse(resourcePlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
    }

    @Test
    public void testKey(){
        String resourceTypeId = "Resource Type";
        EntityType entityType = graknGraph.putEntityType("Entity1");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        RelationType relationType = entityType.key(resourceType);
        assertEquals(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId), relationType.getName());

        Set<String> roleIds = relationType.hasRoles().stream().map(RoleType::getName).collect(Collectors.toSet());
        assertEquals(2, roleIds.size());

        assertTrue(roleIds.contains(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId)));
        assertTrue(roleIds.contains(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId)));

        assertEquals(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId), entityType.playsRoles().iterator().next().getName());
        assertEquals(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId), resourceType.playsRoles().iterator().next().getName());

        //Check everything is implicit
        assertTrue(relationType.isImplicit());
        relationType.hasRoles().forEach(role -> assertTrue(role.isImplicit()));

        // Check that resource is required
        EdgeImpl entityPlays = ((EntityTypeImpl) entityType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertTrue(entityPlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
        EdgeImpl resourcePlays = ((ResourceTypeImpl) resourceType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertTrue(resourcePlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
    }

    @Test
    public void testHasResourceThenKey(){
        EntityType entityType = graknGraph.putEntityType("Entity1");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        RelationType relationTypeHasResource = entityType.hasResource(resourceType);
        RelationType relationTypeKey = entityType.key(resourceType);

        assertEquals(relationTypeHasResource, relationTypeKey);

        // Check that resource is required
        EdgeImpl entityPlays = ((EntityTypeImpl) entityType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertTrue(entityPlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
        EdgeImpl resourcePlays = ((ResourceTypeImpl) resourceType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertTrue(resourcePlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
    }

    @Test
    public void testKeyThenHasResource(){
        EntityType entityType = graknGraph.putEntityType("Entity1");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);

        RelationType relationTypeKey = entityType.key(resourceType);
        RelationType relationTypeHasResource = entityType.hasResource(resourceType);

        assertEquals(relationTypeHasResource, relationTypeKey);

        // Check that resource is required
        EdgeImpl entityPlays = ((EntityTypeImpl) entityType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertTrue(entityPlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
        EdgeImpl resourcePlays = ((ResourceTypeImpl) resourceType).getEdgeOutgoingOfType(Schema.EdgeLabel.PLAYS_ROLE);
        assertTrue(resourcePlays.getPropertyBoolean(Schema.EdgeProperty.REQUIRED));
    }
}