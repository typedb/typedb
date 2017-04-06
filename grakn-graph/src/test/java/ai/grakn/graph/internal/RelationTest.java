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

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.ROLE_IS_NULL;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_DUPLICATE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RelationTest extends GraphTestBase{
    private RelationImpl relation;
    private RoleTypeImpl role1;
    private InstanceImpl rolePlayer1;
    private RoleTypeImpl role2;
    private InstanceImpl rolePlayer2;
    private RoleTypeImpl role3;
    private InstanceImpl rolePlayer3;
    private EntityType type;
    private RelationType relationType;

    private CastingImpl casting1;
    private CastingImpl casting2;

    @Before
    public void buildGraph(){
        role1 = (RoleTypeImpl) graknGraph.putRoleType("Role 1");
        role2 = (RoleTypeImpl) graknGraph.putRoleType("Role 2");
        role3 = (RoleTypeImpl) graknGraph.putRoleType("Role 3");

        type = graknGraph.putEntityType("Main concept Type").plays(role1).plays(role2).plays(role3);
        relationType = graknGraph.putRelationType("Main relation type").relates(role1).relates(role2).relates(role3);

        rolePlayer1 = (InstanceImpl) type.addEntity();
        rolePlayer2 = (InstanceImpl) type.addEntity();
        rolePlayer3 = null;

        relation = (RelationImpl) relationType.addRelation();

        casting1 = graknGraph.addCasting(role1, rolePlayer1, relation);
        casting2 = graknGraph.addCasting(role2, rolePlayer2, relation);
    }

    @Test
    public void whenAddingRolePlayerToRelation_RelationIsExpanded(){
        Relation relation = relationType.addRelation();
        RoleType roleType = graknGraph.putRoleType("A role");
        Instance instance = type.addEntity();

        relation.addRolePlayer(roleType, instance);
        assertEquals(4, relation.allRolePlayers().size());
        assertTrue(relation.allRolePlayers().keySet().contains(roleType));
        assertTrue(relation.rolePlayers().contains(instance));
    }

    @Test
    public void checkShortcutEdgesAreCreatedBetweenAllRolePlayers(){
        //Create the Ontology
        RoleType role1 = graknGraph.putRoleType("Role 1");
        RoleType role2 = graknGraph.putRoleType("Role 2");
        RoleType role3 = graknGraph.putRoleType("Role 3");
        RelationType relType = graknGraph.putRelationType("Rel Type").relates(role1).relates(role2).relates(role3);
        EntityType entType = graknGraph.putEntityType("Entity Type").plays(role1).plays(role2).plays(role3);

        //Data
        EntityImpl entity1r1 = (EntityImpl) entType.addEntity();
        EntityImpl entity2r1 = (EntityImpl) entType.addEntity();
        EntityImpl entity3r2r3 = (EntityImpl) entType.addEntity();
        EntityImpl entity4r3 = (EntityImpl) entType.addEntity();
        EntityImpl entity5r1 = (EntityImpl) entType.addEntity();
        EntityImpl entity6r1r2r3 = (EntityImpl) entType.addEntity();

        //Relation
        Relation relation = relationType.addRelation();
        relation.addRolePlayer(role1, entity1r1);
        relation.addRolePlayer(role1, entity2r1);
        relation.addRolePlayer(role1, entity5r1);
        relation.addRolePlayer(role1, entity6r1r2r3);
        relation.addRolePlayer(role2, entity3r2r3);
        relation.addRolePlayer(role2, entity6r1r2r3);
        relation.addRolePlayer(role3, entity3r2r3);
        relation.addRolePlayer(role3, entity4r3);
        relation.addRolePlayer(role3, entity6r1r2r3);

        //Check the structure of the NEW shortcut edges
        assertThat(followShortcutsToNeighbours(graknGraph, entity1r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followShortcutsToNeighbours(graknGraph, entity2r1),
                containsInAnyOrder(entity2r1, entity1r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followShortcutsToNeighbours(graknGraph, entity3r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followShortcutsToNeighbours(graknGraph, entity4r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followShortcutsToNeighbours(graknGraph, entity5r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followShortcutsToNeighbours(graknGraph, entity6r1r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
    }
    private Set<Concept> followShortcutsToNeighbours(GraknGraph graph, Instance instance) {
        List<Vertex> vertices = graph.admin().getTinkerTraversal().hasId(instance.getId().getRawValue()).
                in(Schema.EdgeLabel.SHORTCUT.getLabel()).
                out(Schema.EdgeLabel.SHORTCUT.getLabel()).toList();

        return vertices.stream().map(vertex -> graph.admin().buildConcept(vertex).asInstance()).collect(Collectors.toSet());
    }

    @Test
    public void whenGettingTheCastingsViaRelation_ReturnCastings() throws Exception {
        assertThat(relation.getMappingCasting(), containsInAnyOrder(casting1, casting2));
    }

    @Test
    public void whenGettingRolePlayersOfRelation_ReturnsRolesAndInstances() throws Exception {
        assertThat(relation.allRolePlayers().keySet(), containsInAnyOrder(role1, role2, role3));
        assertThat(relation.rolePlayers(role1), containsInAnyOrder(rolePlayer1));
        assertThat(relation.rolePlayers(role2), containsInAnyOrder(rolePlayer2));
    }

    @Test
    public void whenCreatingRelation_EnsureUniqueHashIsCreatedBasedOnRolePlayers() throws GraknValidationException {
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").plays(roleType1).plays(roleType2);
        RelationType relationType = graknGraph.putRelationType("relation type").relates(roleType1).relates(roleType2);

        relationType.addRelation();
        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        relation = (RelationImpl) graknGraph.getRelationType("relation type").instances().iterator().next();

        roleType1 = graknGraph.putRoleType("role type 1");
        roleType2 = graknGraph.putRoleType("role type 2");
        Instance instance1 = type.addEntity();

        TreeMap<RoleType, Instance> roleMap = new TreeMap<>();
        roleMap.put(roleType1, instance1);
        roleMap.put(roleType2, null);

        relation.addRolePlayer(roleType1, instance1);
        relation.addRolePlayer(roleType2, null);

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        relation = (RelationImpl) graknGraph.getRelationType("relation type").instances().iterator().next();
        assertEquals(getFakeId(relation.type(), roleMap), relation.getIndex());
    }
    private String getFakeId(RelationType relationType, TreeMap<RoleType, Instance> roleMap){
        String itemIdentifier = "RelationType_" + relationType.getId() + "_Relation";
        for(Map.Entry<RoleType, Instance> entry: roleMap.entrySet()){
            itemIdentifier = itemIdentifier + "_" + entry.getKey().getId();
            if(entry.getValue() != null)
                itemIdentifier = itemIdentifier + "_" + entry.getValue().getId();
        }
        return itemIdentifier;
    }

    @Test
    public void whenAddingDuplicateRelations_Throw() throws GraknValidationException {
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").plays(roleType1).plays(roleType2);
        RelationType relationType = graknGraph.putRelationType("My relation type").relates(roleType1).relates(roleType2);
        Instance instance1 = type.addEntity();
        Instance instance2 = type.addEntity();

        Relation relation1 = relationType.addRelation().addRolePlayer(roleType1, instance1).addRolePlayer(roleType2, instance2);
        relationType.addRelation().addRolePlayer(roleType1, instance1).addRolePlayer(roleType2, instance2);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(VALIDATION_RELATION_DUPLICATE.getMessage(relation1.toString()));

        graknGraph.commit();
    }

    @Test
    public void whenDeletingRelation_CastingsStayBehind(){
        relation.delete();
        assertNotNull(graknGraph.getConcept(casting1.getId()));
        assertNotNull(graknGraph.getConcept(casting2.getId()));
    }

    @Test
    public void ensureRelationToStringContainsRolePlayerInformation(){
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        RelationType relationType = graknGraph.putRelationType("A relation Type").relates(roleType1).relates(roleType2);
        EntityType type = graknGraph.putEntityType("concept type").plays(roleType1).plays(roleType2);
        Instance instance1 = type.addEntity();
        Instance instance2 = type.addEntity();

        Relation relation = relationType.addRelation().addRolePlayer(roleType1, instance1).addRolePlayer(roleType2, instance2);

        String mainDescription = "ID [" + relation.getId() +  "] Type [" + relation.type().getLabel() + "] Roles and Role Players:";
        String rolerp1 = "    Role [" + roleType1.getLabel() + "] played by [" + instance1.getId() + ",]";
        String rolerp2 = "    Role [" + roleType2.getLabel() + "] played by [" + instance2.getId() + ",]";

        assertTrue("Relation toString missing main description", relation.toString().contains(mainDescription));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp1));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp2));
    }

    @Test
    public void whenDeletingRelations_EnsureCastingsRemain(){
        RoleType entityRole = graknGraph.putRoleType("Entity Role");
        RoleType degreeRole = graknGraph.putRoleType("Degree Role");
        EntityType entityType = graknGraph.putEntityType("Entity Type").plays(entityRole);
        ResourceType<Long> degreeType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.LONG).plays(degreeRole);

        RelationType hasDegree = graknGraph.putRelationType("Has Degree").relates(entityRole).relates(degreeRole);

        Entity entity = entityType.addEntity();
        Resource<Long> degree1 = degreeType.putResource(100L);
        Resource<Long> degree2 = degreeType.putResource(101L);

        Relation relation1 = hasDegree.addRelation().addRolePlayer(entityRole, entity).addRolePlayer(degreeRole, degree1);
        hasDegree.addRelation().addRolePlayer(entityRole, entity).addRolePlayer(degreeRole, degree2);

        assertEquals(2, entity.relations().size());

        relation1.delete();

        assertEquals(1, entity.relations().size());
    }


    @Test
    public void whenDeletingFinalInstanceOfRelation_RelationIsDeleted(){
        RoleType roleA = graknGraph.putRoleType("RoleA");
        RoleType roleB = graknGraph.putRoleType("RoleB");
        RoleType roleC = graknGraph.putRoleType("RoleC");

        RelationType relation = graknGraph.putRelationType("relation type").relates(roleA).relates(roleB).relates(roleC);
        EntityType type = graknGraph.putEntityType("concept type").plays(roleA).plays(roleB).plays(roleC);
        Entity a = type.addEntity();
        Entity b = type.addEntity();
        Entity c = type.addEntity();

        ConceptId relationId = relation.addRelation().addRolePlayer(roleA, a).addRolePlayer(roleB, b).addRolePlayer(roleC, c).getId();

        a.delete();
        assertNotNull(graknGraph.getConcept(relationId));
        b.delete();
        assertNotNull(graknGraph.getConcept(relationId));
        c.delete();
        assertNull(graknGraph.getConcept(relationId));
    }

    @Test
    public void whenAddingNullRolePlayerToRelation_Throw(){
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(ROLE_IS_NULL.getMessage(rolePlayer1));

        relationType.addRelation().addRolePlayer(null, rolePlayer1);
    }
}