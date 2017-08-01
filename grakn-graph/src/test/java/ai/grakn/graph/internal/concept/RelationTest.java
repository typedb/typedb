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

package ai.grakn.graph.internal.concept;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.graph.internal.concept.EntityImpl;
import ai.grakn.graph.internal.concept.RelationImpl;
import ai.grakn.graph.internal.concept.RoleImpl;
import ai.grakn.graph.internal.concept.ThingImpl;
import ai.grakn.util.Schema;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RelationTest extends GraphTestBase {
    private RelationImpl relation;
    private RoleImpl role1;
    private ThingImpl rolePlayer1;
    private RoleImpl role2;
    private ThingImpl rolePlayer2;
    private RoleImpl role3;
    private EntityType type;
    private RelationType relationType;

    @Before
    public void buildGraph(){
        role1 = (RoleImpl) graknGraph.putRole("Role 1");
        role2 = (RoleImpl) graknGraph.putRole("Role 2");
        role3 = (RoleImpl) graknGraph.putRole("Role 3");

        type = graknGraph.putEntityType("Main concept Type").plays(role1).plays(role2).plays(role3);
        relationType = graknGraph.putRelationType("Main relation type").relates(role1).relates(role2).relates(role3);

        rolePlayer1 = (ThingImpl) type.addEntity();
        rolePlayer2 = (ThingImpl) type.addEntity();

        relation = (RelationImpl) relationType.addRelation();

        relation.addRolePlayer(role1, rolePlayer1);
        relation.addRolePlayer(role2, rolePlayer2);
    }

    @Test
    public void whenAddingRolePlayerToRelation_RelationIsExpanded(){
        Relation relation = relationType.addRelation();
        Role role = graknGraph.putRole("A role");
        Entity entity1 = type.addEntity();

        relation.addRolePlayer(role, entity1);
        assertThat(relation.allRolePlayers().keySet(), containsInAnyOrder(role1, role2, role3, role));
        assertThat(relation.allRolePlayers().get(role), containsInAnyOrder(entity1));
    }

    @Test
    public void checkShortcutEdgesAreCreatedBetweenAllRolePlayers(){
        //Create the Ontology
        Role role1 = graknGraph.putRole("Role 1");
        Role role2 = graknGraph.putRole("Role 2");
        Role role3 = graknGraph.putRole("Role 3");
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
    private Set<Concept> followShortcutsToNeighbours(GraknGraph graph, Thing thing) {
        List<Vertex> vertices = graph.admin().getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), thing.getId().getValue()).
                in(Schema.EdgeLabel.SHORTCUT.getLabel()).
                out(Schema.EdgeLabel.SHORTCUT.getLabel()).toList();

        return vertices.stream().map(vertex -> graph.admin().buildConcept(vertex).asThing()).collect(Collectors.toSet());
    }

    @Test
    public void whenGettingRolePlayersOfRelation_ReturnsRolesAndInstances() throws Exception {
        assertThat(relation.allRolePlayers().keySet(), containsInAnyOrder(role1, role2, role3));
        assertThat(relation.rolePlayers(role1), containsInAnyOrder(rolePlayer1));
        assertThat(relation.rolePlayers(role2), containsInAnyOrder(rolePlayer2));
    }

    @Test
    public void whenCreatingRelation_EnsureUniqueHashIsCreatedBasedOnRolePlayers() throws InvalidGraphException {
        Role role1 = graknGraph.putRole("role type 1");
        Role role2 = graknGraph.putRole("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").plays(role1).plays(role2);
        RelationType relationType = graknGraph.putRelationType("relation type").relates(role1).relates(role2);

        relationType.addRelation();
        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        relation = (RelationImpl) graknGraph.getRelationType("relation type").instances().iterator().next();

        role1 = graknGraph.putRole("role type 1");
        Thing thing1 = type.addEntity();

        TreeMap<Role, Thing> roleMap = new TreeMap<>();
        roleMap.put(role1, thing1);
        roleMap.put(role2, null);

        relation.addRolePlayer(role1, thing1);

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        relation = (RelationImpl) graknGraph.getRelationType("relation type").instances().iterator().next();
        assertEquals(getFakeId(relation.type(), roleMap), relation.reified().get().getIndex());
    }
    private String getFakeId(RelationType relationType, TreeMap<Role, Thing> roleMap){
        String itemIdentifier = "RelationType_" + relationType.getId() + "_Relation";
        for(Map.Entry<Role, Thing> entry: roleMap.entrySet()){
            itemIdentifier = itemIdentifier + "_" + entry.getKey().getId();
            if(entry.getValue() != null) itemIdentifier += "_" + entry.getValue().getId();
        }
        return itemIdentifier;
    }

    @Test
    public void whenAddingDuplicateRelations_Throw() throws InvalidGraphException {
        Role role1 = graknGraph.putRole("role type 1");
        Role role2 = graknGraph.putRole("role type 2");
        EntityType type = graknGraph.putEntityType("concept type").plays(role1).plays(role2);
        RelationType relationType = graknGraph.putRelationType("My relation type").relates(role1).relates(role2);
        Thing thing1 = type.addEntity();
        Thing thing2 = type.addEntity();

        Relation rel1 = relationType.addRelation().addRolePlayer(role1, thing1).addRolePlayer(role2, thing2);
        Relation rel2 = relationType.addRelation().addRolePlayer(role1, thing1).addRolePlayer(role2, thing2);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(containsString("You have created one or more relations"));

        graknGraph.commit();
    }

    @Test
    public void ensureRelationToStringContainsRolePlayerInformation(){
        Role role1 = graknGraph.putRole("role type 1");
        Role role2 = graknGraph.putRole("role type 2");
        RelationType relationType = graknGraph.putRelationType("A relation Type").relates(role1).relates(role2);
        EntityType type = graknGraph.putEntityType("concept type").plays(role1).plays(role2);
        Thing thing1 = type.addEntity();
        Thing thing2 = type.addEntity();

        Relation relation = relationType.addRelation().addRolePlayer(role1, thing1).addRolePlayer(role2, thing2);

        String mainDescription = "ID [" + relation.getId() +  "] Type [" + relation.type().getLabel() + "] Roles and Role Players:";
        String rolerp1 = "    Role [" + role1.getLabel() + "] played by [" + thing1.getId() + ",]";
        String rolerp2 = "    Role [" + role2.getLabel() + "] played by [" + thing2.getId() + ",]";

        assertTrue("Relation toString missing main description", relation.toString().contains(mainDescription));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp1));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp2));
    }

    @Test
    public void whenDeletingRelations_EnsureCastingsRemain(){
        Role entityRole = graknGraph.putRole("Entity Role");
        Role degreeRole = graknGraph.putRole("Degree Role");
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
        Role roleA = graknGraph.putRole("RoleA");
        Role roleB = graknGraph.putRole("RoleB");
        Role roleC = graknGraph.putRole("RoleC");

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
        expectedException.expect(NullPointerException.class);
        relationType.addRelation().addRolePlayer(null, rolePlayer1);
    }

    @Test
    public void whenAttemptingToLinkTheInstanceOfAResourceRelationToTheResourceWhichCreatedIt_ThrowIfTheRelationTypeDoesNotHavePermissionToPlayTheNecessaryRole(){
        ResourceType<String> resourceType = graknGraph.putResourceType("what a pain", ResourceType.DataType.STRING);
        Resource<String> resource = resourceType.putResource("a real pain");

        EntityType entityType = graknGraph.putEntityType("yay").resource(resourceType);
        Relation implicitRelation = Iterables.getOnlyElement(entityType.addEntity().resource(resource).relations());

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.hasNotAllowed(implicitRelation, resource).getMessage());

        implicitRelation.resource(resource);
    }
}