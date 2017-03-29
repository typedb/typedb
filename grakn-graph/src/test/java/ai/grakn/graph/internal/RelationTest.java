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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.ROLE_IS_NULL;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_DUPLICATE;
import static ai.grakn.util.Schema.EdgeProperty.FROM_ID;
import static ai.grakn.util.Schema.EdgeProperty.FROM_TYPE_NAME;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_ID;
import static ai.grakn.util.Schema.EdgeProperty.RELATION_TYPE_NAME;
import static ai.grakn.util.Schema.EdgeProperty.TO_ID;
import static ai.grakn.util.Schema.EdgeProperty.TO_ROLE_NAME;
import static ai.grakn.util.Schema.EdgeProperty.TO_TYPE_NAME;
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

        type = graknGraph.putEntityType("Main concept Type").playsRole(role1).playsRole(role2).playsRole(role3);
        relationType = graknGraph.putRelationType("Main relation type").hasRole(role1).hasRole(role2).hasRole(role3);

        rolePlayer1 = (InstanceImpl) type.addEntity();
        rolePlayer2 = (InstanceImpl) type.addEntity();
        rolePlayer3 = null;

        relation = (RelationImpl) relationType.addRelation();

        casting1 = graknGraph.putCasting(role1, rolePlayer1, relation);
        casting2 = graknGraph.putCasting(role2, rolePlayer2, relation);
    }

    @Test
    public void whenAddingRolePlayerToRelation_RelationIsExpanded(){
        Relation relation = relationType.addRelation();
        RoleType roleType = graknGraph.putRoleType("A role");
        Instance instance = type.addEntity();

        relation.putRolePlayer(roleType, instance);

        assertThat(relation.rolePlayers().keySet(), containsInAnyOrder(role1, role2, role3, roleType));
        assertThat(relation.rolePlayers().values(), containsInAnyOrder(instance, null, null, null));
        assertEquals(instance, relation.rolePlayers().get(roleType));
    }

    @Test
    public void whenAddingRolePlayersToRelation_EnsureShortcutsAreCreated(){
        EntityImpl a = (EntityImpl) type.addEntity();
        EntityImpl b = (EntityImpl) type.addEntity();
        EntityImpl c = (EntityImpl) type.addEntity();

        assertEquals(0, a.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(0, b.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(0, c.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());

        Relation relation = relationType.addRelation();

        relation.putRolePlayer(role1, a);
        assertEquals(0, a.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(0, b.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(0, c.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());

        relation.putRolePlayer(role2, b);
        assertEquals(1, a.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(1, b.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(0, c.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());

        relation.putRolePlayer(role3, c);
        assertEquals(2, a.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(2, b.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(2, c.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());

        //Check Structure of Shortcut Edge
        a.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT).forEach(edge -> {
            if(edge.getProperty(TO_ROLE_NAME).equals(role2.getName().getValue())) {
                assertEquals(edge.getProperty(RELATION_TYPE_NAME), relationType.getName().getValue());
                assertEquals(edge.getProperty(RELATION_ID), relation.getId().getValue());
                assertEquals(edge.getProperty(TO_ID), b.getId().getValue());
                assertEquals(edge.getProperty(FROM_ID), a.getId().getValue());
                assertEquals(edge.getProperty(FROM_TYPE_NAME), a.type().getName().getValue());
                assertEquals(edge.getProperty(TO_TYPE_NAME), b.type().getName().getValue());
            }
        });
    }

    @Test
    public void whenGettingTheCastingsViaRelation_ReturnCastings() throws Exception {
        assertThat(relation.getMappingCasting(), containsInAnyOrder(casting1, casting2));
    }

    @Test
    public void whenGettingRolePlayersOfRelation_ReturnsRolesAndInstances() throws Exception {
        assertThat(relation.rolePlayers().keySet(), containsInAnyOrder(role1, role2, role3));
        assertThat(relation.rolePlayers().values(), containsInAnyOrder(rolePlayer1, rolePlayer2, rolePlayer3));
    }

    @Test
    public void whenDeletingRelation_EnsureShortcutsAreDeleted() {
        EntityType type = graknGraph.putEntityType("A thing");
        ResourceType<String> resourceType = graknGraph.putResourceType("A resource thing", ResourceType.DataType.STRING);
        EntityImpl instance1 = (EntityImpl) type.addEntity();
        EntityImpl instance2 = (EntityImpl) type.addEntity();
        EntityImpl instance3 = (EntityImpl) type.addEntity();
        ResourceImpl resource = (ResourceImpl) resourceType.putResource("Resource 1");
        RoleType roleType1 = graknGraph.putRoleType("Role 1");
        RoleType roleType2 = graknGraph.putRoleType("Role 2");
        RoleType roleType3 = graknGraph.putRoleType("Role 3");
        RelationType relationType = graknGraph.putRelationType("Relation Type");

        Relation rel = relationType.addRelation().
                putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2).putRolePlayer(roleType3, resource);

        relationType.addRelation().putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance3);

        assertEquals(1, instance1.resources().size());
        assertEquals(2, resource.ownerInstances().size());

        assertEquals(3, instance1.getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(3, instance1.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());

        rel.delete();

        assertEquals(0, instance1.resources().size());
        assertEquals(0, resource.ownerInstances().size());

        assertEquals(1, instance1.getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
        assertEquals(1, instance1.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT).collect(Collectors.toSet()).size());
    }

    @Test
    public void whenCreatingRelation_EnsureUniqueHashIsCreatedBasedOnRolePlayers() throws GraknValidationException {
        RoleType roleType1 = graknGraph.putRoleType("role type 1");
        RoleType roleType2 = graknGraph.putRoleType("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        RelationType relationType = graknGraph.putRelationType("relation type").hasRole(roleType1).hasRole(roleType2);

        relationType.addRelation();
        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.factory(Grakn.IN_MEMORY, graknGraph.getKeyspace()).getGraph();

        relation = (RelationImpl) graknGraph.getRelationType("relation type").instances().iterator().next();

        roleType1 = graknGraph.putRoleType("role type 1");
        roleType2 = graknGraph.putRoleType("role type 2");
        Instance instance1 = type.addEntity();

        TreeMap<RoleType, Instance> roleMap = new TreeMap<>();
        roleMap.put(roleType1, instance1);
        roleMap.put(roleType2, null);

        relation.putRolePlayer(roleType1, instance1);
        relation.putRolePlayer(roleType2, null);

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.factory(Grakn.IN_MEMORY, graknGraph.getKeyspace()).getGraph();

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
        EntityType type = graknGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        RelationType relationType = graknGraph.putRelationType("My relation type").hasRole(roleType1).hasRole(roleType2);
        Instance instance1 = type.addEntity();
        Instance instance2 = type.addEntity();

        Relation relation1 = relationType.addRelation().putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
        relationType.addRelation().putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

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
        RelationType relationType = graknGraph.putRelationType("A relation Type").hasRole(roleType1).hasRole(roleType2);
        EntityType type = graknGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = type.addEntity();
        Instance instance2 = type.addEntity();

        Relation relation = relationType.addRelation().putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        String mainDescription = "ID [" + relation.getId() +  "] Type [" + relation.type().getName() + "] Roles and Role Players:";
        String rolerp1 = "    Role [" + roleType1.getName() + "] played by [" + instance1.getId() + "]";
        String rolerp2 = "    Role [" + roleType2.getName() + "] played by [" + instance2.getId() + "]";

        assertTrue("Relation toString missing main description", relation.toString().contains(mainDescription));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp1));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp2));
    }

    @Test
    public void whenDeletingFinalInstanceOfRelation_RelationIsDeleted(){
        RoleType roleA = graknGraph.putRoleType("RoleA");
        RoleType roleB = graknGraph.putRoleType("RoleB");
        RoleType roleC = graknGraph.putRoleType("RoleC");

        RelationType relation = graknGraph.putRelationType("relation type").hasRole(roleA).hasRole(roleB).hasRole(roleC);
        EntityType type = graknGraph.putEntityType("concept type").playsRole(roleA).playsRole(roleB).playsRole(roleC);
        Entity a = type.addEntity();
        Entity b = type.addEntity();
        Entity c = type.addEntity();

        ConceptId relationId = relation.addRelation().putRolePlayer(roleA, a).putRolePlayer(roleB, b).putRolePlayer(roleC, c).getId();

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

        relationType.addRelation().putRolePlayer(null, rolePlayer1);
    }
}