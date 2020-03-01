/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.concept;

import com.google.common.collect.Iterables;
import grakn.core.common.config.Config;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.answer.Void;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

public class RelationIT {
    private Relation relation;
    private Role role1;
    private Thing rolePlayer1;
    private Role role2;
    private Thing rolePlayer2;
    private Role role3;
    private EntityType type;
    private RelationType relationType;

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        tx = session.writeTransaction();
        role1 = tx.putRole("Role 1");
        role2 = tx.putRole("Role 2");
        role3 = tx.putRole("Role 3");

        type = tx.putEntityType("Main concept Type").plays(role1).plays(role2).plays(role3);
        relationType = tx.putRelationType("Main relation type").relates(role1).relates(role2).relates(role3);

        rolePlayer1 = type.create();
        rolePlayer2 = type.create();

        relation = relationType.create();

        relation.assign(role1, rolePlayer1);
        relation.assign(role2, rolePlayer2);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenAddingRolePlayerToRelation_RelationIsExpanded(){
        Relation relation = relationType.create();
        Role role = tx.putRole("A role");
        Entity entity1 = type.create();

        relation.assign(role, entity1);
        assertThat(relation.rolePlayersMap().keySet(), containsInAnyOrder(role1, role2, role3, role));
        assertThat(relation.rolePlayersMap().get(role), containsInAnyOrder(entity1));
    }

    @Test
    public void whenCreatingAnInferredRelation_EnsureMarkedAsInferred(){
        RelationType rt = tx.putRelationType("rt");
        Relation relation = rt.create();
        Relation relationInferred = rt.addRelationInferred();
        assertFalse(relation.isInferred());
        assertTrue(relationInferred.isInferred());
    }

    @Test
    public void checkRolePlayerEdgesAreCreatedBetweenAllRolePlayers(){
        // for this test, we need TestTransaction which we can safely downcast
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction) tx);

        //Create the Schema
        Role role1 = testTx.putRole("Role 1");
        Role role2 = testTx.putRole("Role 2");
        Role role3 = testTx.putRole("Role 3");
        testTx.putRelationType("Rel Type").relates(role1).relates(role2).relates(role3);
        EntityType entType = testTx.putEntityType("Entity Type").plays(role1).plays(role2).plays(role3);

        //Data
        Entity entity1r1 = (Entity) entType.create();
        Entity entity2r1 = (Entity) entType.create();
        Entity entity3r2r3 = (Entity) entType.create();
        Entity entity4r3 = (Entity) entType.create();
        Entity entity5r1 = (Entity) entType.create();
        Entity entity6r1r2r3 = (Entity) entType.create();

        //Relation
        Relation relation = relationType.create();
        relation.assign(role1, entity1r1);
        relation.assign(role1, entity2r1);
        relation.assign(role1, entity5r1);
        relation.assign(role1, entity6r1r2r3);
        relation.assign(role2, entity3r2r3);
        relation.assign(role2, entity6r1r2r3);
        relation.assign(role3, entity3r2r3);
        relation.assign(role3, entity4r3);
        relation.assign(role3, entity6r1r2r3);

        //Check the structure of the NEW role-player edges
        assertThat(followRolePlayerEdgesToNeighbours(testTx, entity1r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(testTx, entity2r1),
                containsInAnyOrder(entity2r1, entity1r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(testTx, entity3r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(testTx, entity4r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(testTx, entity5r1),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
        assertThat(followRolePlayerEdgesToNeighbours(testTx, entity6r1r2r3),
                containsInAnyOrder(entity1r1, entity2r1, entity3r2r3, entity4r3, entity5r1, entity6r1r2r3));
    }
    private Set<Concept> followRolePlayerEdgesToNeighbours(TestTransactionProvider.TestTransaction tx, Thing thing) {
        JanusTraversalSourceProvider janusTraversalSourceProvider = tx.janusTraversalSourceProvider();
        ConceptManager conceptManager = tx.conceptManager();
        List<Vertex> vertices = janusTraversalSourceProvider.getTinkerTraversal()
                .V()
                .hasId(Schema.elementId(thing.id()))
                .in(Schema.EdgeLabel.ROLE_PLAYER.getLabel())
                .out(Schema.EdgeLabel.ROLE_PLAYER.getLabel())
                .toList();

        return vertices.stream().map(vertex -> conceptManager.buildConcept(vertex).asThing()).collect(Collectors.toSet());
    }

    @Test
    public void whenGettingRolePlayersOfRelation_ReturnsRolesAndInstances() throws Exception {
        assertThat(relation.rolePlayersMap().keySet(), Matchers.containsInAnyOrder(role1, role2, role3));
        assertThat(relation.rolePlayers(role1).collect(toSet()), containsInAnyOrder(rolePlayer1));
        assertThat(relation.rolePlayers(role2).collect(toSet()), containsInAnyOrder(rolePlayer2));
    }

    @Test
    public void ensureRelationToStringContainsRolePlayerInformation(){
        Role role1 = tx.putRole("role type 1");
        Role role2 = tx.putRole("role type 2");
        RelationType relationType = tx.putRelationType("A relation Type").relates(role1).relates(role2);
        EntityType type = tx.putEntityType("concept type").plays(role1).plays(role2);
        Thing thing1 = type.create();
        Thing thing2 = type.create();

        Relation relation = relationType.create().assign(role1, thing1).assign(role2, thing2);

        String mainDescription = "ID [" + relation.id() +  "] Type [" + relation.type().label() + "] Roles and Role Players:";
        String rolerp1 = "    Role [" + role1.label() + "] played by [" + thing1.id() + ",]";
        String rolerp2 = "    Role [" + role2.label() + "] played by [" + thing2.id() + ",]";

        assertTrue("Relation toString missing main description", relation.toString().contains(mainDescription));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp1));
        assertTrue("Relation toString missing role and role player definition", relation.toString().contains(rolerp2));
    }

    @Test
    public void whenDeletingRelations_EnsureCastingsRemain(){
        Role entityRole = tx.putRole("Entity Role");
        Role degreeRole = tx.putRole("Degree Role");
        EntityType entityType = tx.putEntityType("Entity Type").plays(entityRole);
        AttributeType<Long> degreeType = tx.putAttributeType("Attribute Type", AttributeType.DataType.LONG).plays(degreeRole);

        RelationType hasDegree = tx.putRelationType("Has Degree").relates(entityRole).relates(degreeRole);

        Entity entity = entityType.create();
        Attribute<Long> degree1 = degreeType.create(100L);
        Attribute<Long> degree2 = degreeType.create(101L);

        Relation relation1 = hasDegree.create().assign(entityRole, entity).assign(degreeRole, degree1);
        hasDegree.create().assign(entityRole, entity).assign(degreeRole, degree2);

        assertEquals(2, entity.relations().count());

        relation1.delete();

        assertEquals(1, entity.relations().count());
    }


    @Test
    public void whenDeletingFinalInstanceOfRelation_RelationIsDeleted(){
        Role roleA = tx.putRole("RoleA");
        Role roleB = tx.putRole("RoleB");
        Role roleC = tx.putRole("RoleC");

        RelationType relation = tx.putRelationType("relation type").relates(roleA).relates(roleB).relates(roleC);
        EntityType type = tx.putEntityType("concept type").plays(roleA).plays(roleB).plays(roleC);
        Entity a = type.create();
        Entity b = type.create();
        Entity c = type.create();

        ConceptId relationId = relation.create().assign(roleA, a).assign(roleB, b).assign(roleC, c).id();

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
        relationType.create().assign(null, rolePlayer1);
    }

    @Test
    public void whenAttemptingToLinkTheInstanceOfAResourceRelationToTheResourceWhichCreatedIt_ThrowIfTheRelationTypeDoesNotHavePermissionToPlayTheNecessaryRole(){
        AttributeType<String> attributeType = tx.putAttributeType("what a pain", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("a real pain");

        EntityType entityType = tx.putEntityType("yay").has(attributeType);
        Relation implicitRelation = Iterables.getOnlyElement(entityType.create().has(attribute).relations().collect(Collectors.toSet()));

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.hasNotAllowed(implicitRelation, attribute).getMessage());

        implicitRelation.has(attribute);
    }


    @Test
    public void whenAddingDuplicateRelationsWithDifferentKeys_EnsureTheyCanBeCommitted(){
        Role role1 = tx.putRole("dark");
        Role role2 = tx.putRole("souls");
        AttributeType<Long> attributeType = tx.putAttributeType("Death Number", AttributeType.DataType.LONG);
        RelationType relationType = tx.putRelationType("Dark Souls").relates(role1).relates(role2).key(attributeType);
        EntityType entityType = tx.putEntityType("Dead Guys").plays(role1).plays(role2);

        Entity e1 = entityType.create();
        Entity e2 = entityType.create();

        Attribute<Long> r1 = attributeType.create(1000000L);
        Attribute<Long> r2 = attributeType.create(2000000L);

        Relation rel1 = relationType.create().assign(role1, e1).assign(role2, e2);
        Relation rel2 = relationType.create().assign(role1, e1).assign(role2, e2);

        //Set the keys and commit. Without this step it should fail
        rel1.has(r1);
        rel2.has(r2);

        tx.commit();
        tx = session.writeTransaction();

        assertThat(tx.getMetaRelationType().instances().collect(toSet()), Matchers.hasItem(rel1));
        assertThat(tx.getMetaRelationType().instances().collect(toSet()), Matchers.hasItem(rel2));
    }

    @Test
    public void whenRemovingRolePlayerFromRelation_EnsureRolePlayerIsRemoved(){
        Role role1 = tx.putRole("dark");
        Role role2 = tx.putRole("souls");
        RelationType relationType = tx.putRelationType("Dark Souls").relates(role1).relates(role2);
        EntityType entityType = tx.putEntityType("Dead Guys").plays(role1).plays(role2);

        Entity e1 = entityType.create();
        Entity e2 = entityType.create();
        Entity e3 = entityType.create();
        Entity e4 = entityType.create();
        Entity e5 = entityType.create();
        Entity e6 = entityType.create();

        Relation relation = relationType.create().
                assign(role1, e1).assign(role1, e2).assign(role1, e3).
                assign(role2, e4).assign(role2, e5).assign(role2, e6);

        assertThat(relation.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e2, e3, e4, e5, e6));
        relation.unassign(role1, e2);
        relation.unassign(role2, e1);
        assertThat(relation.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e3, e4, e5, e6));
        relation.unassign(role2, e6);
        assertThat(relation.rolePlayers().collect(Collectors.toSet()), containsInAnyOrder(e1, e3, e4, e5));
    }

    @Test
    public void whenAttributeLinkedToRelationIsInferred_EnsureItIsMarkedAsInferred(){
        AttributeType attributeType = tx.putAttributeType("Another thing of sorts", AttributeType.DataType.STRING);
        RelationType relationType = tx.putRelationType("A thing of sorts").has(attributeType);

        Attribute attribute = attributeType.create("Things");
        Relation relation = relationType.create();

        relation.attributeInferred(attribute);
        assertTrue(relation.relations().findAny().get().isInferred());
    }

    @Test
    public void whenAddingRelationWithNoRolePlayers_Throw(){
        Role role1 = tx.putRole("r1");
        Role role2 = tx.putRole("r2");
        RelationType relationType = tx.putRelationType("A thing of sorts").relates(role1).relates(role2);
        Relation relation = relationType.create();

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_WITH_NO_ROLE_PLAYERS.getMessage(relation.id(), relation.type().label())));

        tx.commit();
    }

    @Test
    public void whenUnattachingAttributeFromRelation_operationSucceeds(){

        Role member = tx.putRole("member");
        Role member_of = tx.putRole("member_of");
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        RelationType membership = tx.putRelationType("membership")
                .relates(member)
                .relates(member_of)
                .has(name);
        EntityType group = tx.putEntityType("group").plays(member_of);
        EntityType person = tx.putEntityType("person").plays(member);

        Entity personInst = person.create();
        Entity groupInst = group.create();

        Attribute<String> attr = name.create("founder");
        Relation membershipInst = membership.create()
                .assign(member, personInst)
                .assign(member_of, groupInst)
                .has(attr);

        membershipInst.unhas(attr);
        assertFalse(membershipInst.attributes().findFirst().isPresent());
    }

    @Test
    public void whenDeletingInferredRelationship_NoErrorIsThrown() {
        /*
        The exact behavior is up for debate, but at the very least we should not
        throw an exception when deleting an inferred concept, otherwise there is no way to delete
        concrete instances from a mix of inferred and concrete concepts.
        */

        String schema = "define " +
                "person sub entity, plays mother, plays sister, plays son, plays nephew, plays aunt; " +
                "motherhood sub relation, relates mother, relates son;" +
                "sisterhood sub relation, relates sister;" +
                "aunthood sub relation, relates aunt, relates nephew, has number;" +
                "number sub attribute, datatype long;" +
                "auntie-rule sub rule, " +
                "when { " +
                "$mother isa person; " +
                "$sister isa person; " +
                "$son isa person; " +
                "(sister: $mother, sister: $sister) isa sisterhood; " +
                "(mother: $mother, son: $son) isa motherhood; " +
                "}, then {" +
                "(aunt: $sister, nephew: $son) isa aunthood; };";

        // create the schema with the rule
        GraqlDefine define = Graql.parse(schema).asDefine();
        tx.execute(define);

        // insert some data that triggers the rule
        GraqlInsert insert = Graql.parse( "insert $m isa person; $s isa person; $d isa person; " +
                "(sister: $m, sister: $s) isa sisterhood; " +
                "(mother: $m, son: $d) isa motherhood;").asInsert();
        tx.execute(insert);
        tx.commit();

        // try to delete the inferred relationship
        tx = session.writeTransaction();
        GraqlDelete delete = Graql.parse("match $r isa aunthood; delete $r;").asDelete();
        List<Void> deletedConcepts = tx.execute(delete);

        // normally throws on commit
        tx.commit();
    }
}