/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal.concept;

import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RelationshipTest extends TxTestBase {
    private RelationshipImpl relation;
    private RoleImpl role1;
    private ThingImpl rolePlayer1;
    private RoleImpl role2;
    private ThingImpl rolePlayer2;
    private RoleImpl role3;
    private EntityType type;
    private RelationshipType relationshipType;

    @Before
    public void setup(){
        role1 = (RoleImpl) tx.putRole("Role 1");
        role2 = (RoleImpl) tx.putRole("Role 2");
        role3 = (RoleImpl) tx.putRole("Role 3");

        type = tx.putEntityType("Main concept Type").plays(role1).plays(role2).plays(role3);
        relationshipType = tx.putRelationshipType("Main relation type").relates(role1).relates(role2).relates(role3);

        rolePlayer1 = (ThingImpl) type.addEntity();
        rolePlayer2 = (ThingImpl) type.addEntity();

        relation = (RelationshipImpl) relationshipType.addRelationship();

        relation.addRolePlayer(role1, rolePlayer1);
        relation.addRolePlayer(role2, rolePlayer2);
    }

    @Test
    public void whenAddingRolePlayerToRelation_RelationIsExpanded(){
        Relationship relationship = relationshipType.addRelationship();
        Role role = tx.putRole("A role");
        Entity entity1 = type.addEntity();

        relationship.addRolePlayer(role, entity1);
        assertThat(relationship.allRolePlayers().keySet(), containsInAnyOrder(role1, role2, role3, role));
        assertThat(relationship.allRolePlayers().get(role), containsInAnyOrder(entity1));
    }

    @Test
    public void whenCreatingAnInferredRelationship_EnsureMarkedAsInferred(){
        RelationshipTypeImpl rt = RelationshipTypeImpl.from(tx.putRelationshipType("rt"));
        Relationship relationship = rt.addRelationship();
        Relationship relationshipInferred = rt.addRelationshipInferred();
        assertFalse(relationship.isInferred());
        assertTrue(relationshipInferred.isInferred());
    }

    @Test
    public void checkRolePlayerEdgesAreCreatedBetweenAllRolePlayers(){
        //Create the Schema
        Role role1 = tx.putRole("Role 1");
        Role role2 = tx.putRole("Role 2");
        Role role3 = tx.putRole("Role 3");
        tx.putRelationshipType("Rel Type").relates(role1).relates(role2).relates(role3);
        EntityType entType = tx.putEntityType("Entity Type").plays(role1).plays(role2).plays(role3);

        //Data
        EntityImpl entity1r1 = (EntityImpl) entType.addEntity();
        EntityImpl entity2r1 = (EntityImpl) entType.addEntity();
        EntityImpl entity3r2r3 = (EntityImpl) entType.addEntity();
        EntityImpl entity4r3 = (EntityImpl) entType.addEntity();
        EntityImpl entity5r1 = (EntityImpl) entType.addEntity();
        EntityImpl entity6r1r2r3 = (EntityImpl) entType.addEntity();

        //Relationship
        Relationship relationship = relationshipType.addRelationship();
        relationship.addRolePlayer(role1, entity1r1);
        relationship.addRolePlayer(role1, entity2r1);
        relationship.addRolePlayer(role1, entity5r1);
        relationship.addRolePlayer(role1, entity6r1r2r3);
        relationship.addRolePlayer(role2, entity3r2r3);
        relationship.addRolePlayer(role2, entity6r1r2r3);
        relationship.addRolePlayer(role3, entity3r2r3);
        relationship.addRolePlayer(role3, entity4r3);
        relationship.addRolePlayer(role3, entity6r1r2r3);

        //Check the structure of the NEW role-player edges
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity1r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity2r1),
                containsInAnyOrder(entity2r1, entity1r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity3r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity4r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity5r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(tx, entity6r1r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
    }
    private Set<Concept> followRolePlayerEdgesToNeighbours(EmbeddedGraknTx<?> tx, Thing thing) {
        List<Vertex> vertices = tx.getTinkerTraversal().V().has(Schema.VertexProperty.ID.name(), thing.getId().getValue()).
                in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                out(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).toList();

        return vertices.stream().map(vertex -> tx.buildConcept(vertex).asThing()).collect(Collectors.toSet());
    }

    @Test
    public void whenGettingRolePlayersOfRelation_ReturnsRolesAndInstances() throws Exception {
        assertThat(relation.allRolePlayers().keySet(), Matchers.containsInAnyOrder(role1, role2, role3));
        assertThat(relation.rolePlayers(role1).collect(toSet()), containsInAnyOrder(rolePlayer1));
        assertThat(relation.rolePlayers(role2).collect(toSet()), containsInAnyOrder(rolePlayer2));
    }

    @Test
    public void ensureRelationToStringContainsRolePlayerInformation(){
        Role role1 = tx.putRole("role type 1");
        Role role2 = tx.putRole("role type 2");
        RelationshipType relationshipType = tx.putRelationshipType("A relationship Type").relates(role1).relates(role2);
        EntityType type = tx.putEntityType("concept type").plays(role1).plays(role2);
        Thing thing1 = type.addEntity();
        Thing thing2 = type.addEntity();

        Relationship relationship = relationshipType.addRelationship().addRolePlayer(role1, thing1).addRolePlayer(role2, thing2);

        String mainDescription = "ID [" + relationship.getId() +  "] Type [" + relationship.type().getLabel() + "] Roles and Role Players:";
        String rolerp1 = "    Role [" + role1.getLabel() + "] played by [" + thing1.getId() + ",]";
        String rolerp2 = "    Role [" + role2.getLabel() + "] played by [" + thing2.getId() + ",]";

        assertTrue("Relationship toString missing main description", relationship.toString().contains(mainDescription));
        assertTrue("Relationship toString missing role and role player definition", relationship.toString().contains(rolerp1));
        assertTrue("Relationship toString missing role and role player definition", relationship.toString().contains(rolerp2));
    }

    @Test
    public void whenDeletingRelations_EnsureCastingsRemain(){
        Role entityRole = tx.putRole("Entity Role");
        Role degreeRole = tx.putRole("Degree Role");
        EntityType entityType = tx.putEntityType("Entity Type").plays(entityRole);
        AttributeType<Long> degreeType = tx.putAttributeType("Attribute Type", AttributeType.DataType.LONG).plays(degreeRole);

        RelationshipType hasDegree = tx.putRelationshipType("Has Degree").relates(entityRole).relates(degreeRole);

        Entity entity = entityType.addEntity();
        Attribute<Long> degree1 = degreeType.putAttribute(100L);
        Attribute<Long> degree2 = degreeType.putAttribute(101L);

        Relationship relationship1 = hasDegree.addRelationship().addRolePlayer(entityRole, entity).addRolePlayer(degreeRole, degree1);
        hasDegree.addRelationship().addRolePlayer(entityRole, entity).addRolePlayer(degreeRole, degree2);

        assertEquals(2, entity.relationships().count());

        relationship1.delete();

        assertEquals(1, entity.relationships().count());
    }


    @Test
    public void whenDeletingFinalInstanceOfRelation_RelationIsDeleted(){
        Role roleA = tx.putRole("RoleA");
        Role roleB = tx.putRole("RoleB");
        Role roleC = tx.putRole("RoleC");

        RelationshipType relation = tx.putRelationshipType("relation type").relates(roleA).relates(roleB).relates(roleC);
        EntityType type = tx.putEntityType("concept type").plays(roleA).plays(roleB).plays(roleC);
        Entity a = type.addEntity();
        Entity b = type.addEntity();
        Entity c = type.addEntity();

        ConceptId relationId = relation.addRelationship().addRolePlayer(roleA, a).addRolePlayer(roleB, b).addRolePlayer(roleC, c).getId();

        a.delete();
        assertNotNull(tx.getConcept(relationId));
        b.delete();
        assertNotNull(tx.getConcept(relationId));
        c.delete();
        assertNull(tx.getConcept(relationId));
    }

    @Test
    public void whenAddingNullRolePlayerToRelation_Throw(){
        expectedException.expect(NullPointerException.class);
        relationshipType.addRelationship().addRolePlayer(null, rolePlayer1);
    }

    @Test
    public void whenAttemptingToLinkTheInstanceOfAResourceRelationToTheResourceWhichCreatedIt_ThrowIfTheRelationTypeDoesNotHavePermissionToPlayTheNecessaryRole(){
        AttributeType<String> attributeType = tx.putAttributeType("what a pain", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putAttribute("a real pain");

        EntityType entityType = tx.putEntityType("yay").attribute(attributeType);
        Relationship implicitRelationship = Iterables.getOnlyElement(entityType.addEntity().attribute(attribute).relationships().collect(Collectors.toSet()));

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.hasNotAllowed(implicitRelationship, attribute).getMessage());

        implicitRelationship.attribute(attribute);
    }


    @Test
    public void whenAddingDuplicateRelationsWithDifferentKeys_EnsureTheyCanBeCommitted(){
        Role role1 = tx.putRole("dark");
        Role role2 = tx.putRole("souls");
        AttributeType<Long> attributeType = tx.putAttributeType("Death Number", AttributeType.DataType.LONG);
        RelationshipType relationshipType = tx.putRelationshipType("Dark Souls").relates(role1).relates(role2).key(attributeType);
        EntityType entityType = tx.putEntityType("Dead Guys").plays(role1).plays(role2);

        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();

        Attribute<Long> r1 = attributeType.putAttribute(1000000L);
        Attribute<Long> r2 = attributeType.putAttribute(2000000L);

        Relationship rel1 = relationshipType.addRelationship().addRolePlayer(role1, e1).addRolePlayer(role2, e2);
        Relationship rel2 = relationshipType.addRelationship().addRolePlayer(role1, e1).addRolePlayer(role2, e2);

        //Set the keys and commit. Without this step it should fail
        rel1.attribute(r1);
        rel2.attribute(r2);

        tx.commit();
        tx = session.open(GraknTxType.WRITE);

        assertThat(tx.admin().getMetaRelationType().instances().collect(toSet()), Matchers.hasItem(rel1));
        assertThat(tx.admin().getMetaRelationType().instances().collect(toSet()), Matchers.hasItem(rel2));
    }

    @Test
    public void whenRemovingRolePlayerFromRelationship_EnsureRolePlayerIsRemoved(){
        Role role1 = tx.putRole("dark");
        Role role2 = tx.putRole("souls");
        RelationshipType relationshipType = tx.putRelationshipType("Dark Souls").relates(role1).relates(role2);
        EntityType entityType = tx.putEntityType("Dead Guys").plays(role1).plays(role2);

        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        Entity e3 = entityType.addEntity();
        Entity e4 = entityType.addEntity();
        Entity e5 = entityType.addEntity();
        Entity e6 = entityType.addEntity();

        Relationship relationship = relationshipType.addRelationship().
                addRolePlayer(role1, e1).addRolePlayer(role1, e2).addRolePlayer(role1, e3).
                addRolePlayer(role2, e4).addRolePlayer(role2, e5).addRolePlayer(role2, e6);

        assertThat(relationship.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e2, e3, e4, e5, e6));
        relationship.removeRolePlayer(role1, e2);
        relationship.removeRolePlayer(role2, e1);
        assertThat(relationship.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e3, e4, e5, e6));
        relationship.removeRolePlayer(role2, e6);
        assertThat(relationship.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e3, e4, e5));
    }

    @Test
    public void whenAttributeLinkedToRelationshipIsInferred_EnsureItIsMarkedAsInferred(){
        AttributeType attributeType = tx.putAttributeType("Another thing of sorts", AttributeType.DataType.STRING);
        RelationshipType relationshipType = tx.putRelationshipType("A thing of sorts").attribute(attributeType);

        Attribute attribute = attributeType.putAttribute("Things");
        Relationship relationship = relationshipType.addRelationship();

        RelationshipImpl.from(relationship).attributeInferred(attribute);
        assertTrue(relationship.relationships().findAny().get().isInferred());
    }

    @Test
    public void whenAddingRelationshipWithNoRolePlayers_Throw(){
        Role role1 = tx.putRole("r1");
        Role role2 = tx.putRole("r2");
        RelationshipType relationshipType = tx.putRelationshipType("A thing of sorts").relates(role1).relates(role2);
        Relationship relationship = relationshipType.addRelationship();

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATIONSHIP_WITH_NO_ROLE_PLAYERS.getMessage(relationship.getId(), relationship.type().getLabel())));

        tx.commit();
    }
}