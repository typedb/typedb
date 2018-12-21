/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.kb;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ValidatorIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenCreatingAbstractRelationshipWithSubType_EnsureValidationRuleForMatchingSubRolesIsSkipped(){
        Role role1 = tx.putRole("my role");
        Role role2 = tx.putRole("my role 2");

        RelationType abstractRelationType = tx.putRelationshipType("my abstract relation type").
                relates(role1).
                isAbstract(true);
        tx.putRelationshipType("my relation type").
                sup(abstractRelationType).
                relates(role2);

        tx.commit();
    }

    @Test
    public void whenCommittingGraphWhichFollowsValidationRules_Commit(){
        RelationType cast = tx.putRelationshipType("Cast");
        Role feature = tx.putRole("Feature");
        Role actor = tx.putRole("Actor");
        EntityType movie = tx.putEntityType("Movie");
        EntityType person = tx.putEntityType("Person");
        Thing pacino = person.create();
        Thing godfather = movie.create();
        EntityType genre = tx.putEntityType("Genre");
        Role movieOfGenre = tx.putRole("Movie of Genre");
        Role movieGenre = tx.putRole("Movie Genre");
        Thing crime = genre.create();
        RelationType movieHasGenre = tx.putRelationshipType("Movie Has Genre");

        //Construction
        cast.relates(feature);
        cast.relates(actor);

        cast.create().
                assign(feature, godfather).assign(actor, pacino);

        movieHasGenre.create().
                assign(movieOfGenre, godfather).assign(movieGenre, crime);

        movieHasGenre.relates(movieOfGenre);
        movieHasGenre.relates(movieGenre);

        movie.plays(movieOfGenre);
        person.plays(actor);
        movie.plays(feature);
        genre.plays(movieGenre);

        tx.commit();
    }

    @Test
    public void whenCommittingRelationWithoutSpecifyingSchema_ThrowOnCommit(){
        EntityType fakeType = tx.putEntityType("Fake Concept");
        RelationType relationshipType = tx.putRelationshipType("kicks");
        Role kicker = tx.putRole("kicker");
        Role kickee = tx.putRole("kickee");
        Thing kyle = fakeType.create();
        Thing icke = fakeType.create();

        relationshipType.create().assign(kicker, kyle).assign(kickee, icke);

        String error1 = ErrorMessage.VALIDATION_CASTING.getMessage(kyle.type().label(), kyle.id(), kicker.label());
        String error2 = ErrorMessage.VALIDATION_CASTING.getMessage(icke.type().label(), icke.id(), kickee.label());

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(allOf(containsString(error1), containsString(error2)));

        tx.commit();
    }

    @Test
    public void whenCommittingNonAbstractRoleTypeNotLinkedToAnyRelationType_Throw(){
        Role alone = tx.putRole("alone");

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(alone.label())));

        tx.commit();
    }

    @Test
    public void whenCommittingNonAbstractRelationTypeNotLinkedToAnyRoleType_Throw(){
        RelationType alone = tx.putRelationshipType("alone");

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(alone.label())));

        tx.commit();
    }

    @Test
    public void whenCreatingRelationWithoutLinkingRelates_Throw(){
        Role hunter = tx.putRole("hunter");
        Role monster = tx.putRole("monster");
        EntityType stuff = tx.putEntityType("Stuff").plays(hunter).plays(monster);
        RelationType kills = tx.putRelationshipType("kills").relates(hunter);

        Entity myHunter = stuff.create();
        Entity myMonster = stuff.create();

        Relation relation = kills.create().assign(hunter, myHunter).assign(monster, myMonster);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relation.id(), monster.label(), kills.label())));

        tx.commit();
    }

    @Test
    public void whenDeletingRelations_EnsureGraphRemainsValid() throws InvalidKBException {
        // schema
        EntityType person = tx.putEntityType("person");
        EntityType movie = tx.putEntityType("movie");
        RelationType cast = tx.putRelationshipType("cast");
        Role feature = tx.putRole("feature");
        Role actor = tx.putRole("actor");
        cast.relates(feature).relates(actor);
        person.plays(actor);
        movie.plays(feature);

        // add a single movie
        Thing godfather = movie.create();

        // add many random actors
        int n = 50;
        for (int i=0; i < n; i++) {
            Thing newPerson = person.create();
            cast.create().
                    assign(actor, newPerson).assign(feature, godfather);
        }

        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);

        // now try to delete all assertions and then the movie
        godfather = tx.getEntityType("movie").instances().iterator().next();
        Collection<Relation> assertions = godfather.relationships().collect(Collectors.toSet());
        Set<ConceptId> assertionIds = new HashSet<>();

        for (Relation a : assertions) {
            assertionIds.add(a.id());
            a.delete();
        }
        godfather.delete();

        tx.commit();
        tx = session.transaction(Transaction.Type.WRITE);

        assertionIds.forEach(id -> assertNull(tx.getConcept(id)));

        // assert the movie is gone
        assertNull(tx.getEntityType("godfather"));
    }

    @Test
    public void whenManuallyCreatingCorrectBinaryRelation_Commit() throws InvalidKBException {
        Role characterBeingPlayed = tx.putRole("Character being played");
        Role personPlayingCharacter = tx.putRole("Person Playing Char");
        RelationType playsChar = tx.putRelationshipType("Plays Char").relates(characterBeingPlayed).relates(personPlayingCharacter);

        EntityType person = tx.putEntityType("person").plays(characterBeingPlayed).plays(personPlayingCharacter);
        EntityType character = tx.putEntityType("character").plays(characterBeingPlayed);

        Entity matt = person.create();
        Entity walker = character.create();

        playsChar.create().
                assign(personPlayingCharacter, matt).
                assign(characterBeingPlayed, walker);

        tx.commit();
    }

    /*------------------------------- Entity Type to Role Type Validation (Schema) -----------------------------------*/
    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureEntityTypesPlayAllRolesExplicitly1() throws InvalidKBException {
        Role relative = tx.putRole("relative");
        Role parent = tx.putRole("parent").sup(relative);
        Role father = tx.putRole("father").sup(parent);
        Role mother = tx.putRole("mother").sup(parent);

        EntityType person = tx.putEntityType("person").plays(relative).plays(parent);
        tx.putEntityType("man").sup(person).plays(father);
        tx.putEntityType("woman").sup(person).plays(mother);

        Role child = tx.putRole("child");

        //Padding to make it valid
        tx.putRelationshipType("filler").relates(parent).relates(child).relates(father).relates(relative).relates(mother);

        tx.commit();
    }

    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureEntityTypesPlayAllRolesExplicitly2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType company = tx.putEntityType("company").plays(parent);
        tx.putEntityType("companySub").sup(company).plays(child);
        tx.putEntityType("person").plays(parent).plays(child);

        //Padding to make it valid
        tx.putRelationshipType("filler").relates(parent).relates(child);

        tx.commit();
    }
    /*-------------------------------- Entity Type to Role Type Validation (Data) ------------------------------------*/

    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureInstancesCanPlayRelevantRoles1() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(parent).plays(child);
        EntityType man = tx.putEntityType("man").sup(person);
        EntityType oneEyedMan = tx.putEntityType("oneEyedMan").sup(man);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = oneEyedMan.create();
        Entity y = person.create();

        parenthood.create().assign(parent, x).assign(child, y);

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureInstancesCanPlayRelevantRoles2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(parent).plays(child);
        EntityType company = tx.putEntityType("company").plays(parent);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = company.create();
        Entity y = person.create();

        parenthood.create().assign(parent, x).assign(child, y);

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw1() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(parent).plays(child);
        EntityType man = tx.putEntityType("man");

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = man.create();
        Entity y = person.create();

        parenthood.create().assign(parent, x).assign(child, y);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(man.label(), x.id(), parent.label()));

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(child);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = person.create();
        Entity y = person.create();

        parenthood.create().assign(parent, x).assign(child, y);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.label(), x.id(), parent.label()));

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw3() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(child);
        tx.putEntityType("man").plays(child);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = person.create();
        Entity y = person.create();
        parenthood.create().assign(parent, x).assign(child, y);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.label(), x.id(), parent.label()));

        tx.commit();
    }

    /*------------------------------- Relationship Type to Role Type Validation (Schema) ---------------------------------*/
    @Test
    public void whenARelationTypeHasASubTypeHierarchy_EnsureThatWhenARelationTypeHasMatchingRoleTypes1() throws InvalidKBException {
        Role relative = tx.putRole("relative");
        Role parent = tx.putRole("parent").sup(relative);
        Role father = tx.putRole("father").sup(parent);
        Role mother = tx.putRole("mother").sup(parent);
        Role pChild = tx.putRole("pChild").sup(relative);
        Role fChild = tx.putRole("fChild").sup(pChild);
        Role mChild = tx.putRole("mChild").sup(pChild);

        //This is to bypass a specific validation rule
        tx.putRelationshipType("filler").relates(relative);

        tx.putEntityType("animal").
                plays(relative).
                plays(parent).
                plays(father).
                plays(mother).
                plays(pChild).
                plays(fChild).
                plays(mChild);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild);
        tx.putRelationshipType("fatherhood").sup(parenthood).relates(father).relates(fChild);
        tx.putRelationshipType("motherhood").sup(parenthood).relates(mother).relates(mChild);

        tx.commit();
    }

    @Test
    public void whenARelationTypeHasASubTypeHierarchy_EnsureThatWhenARelationTypeHasMatchingRoleTypes2() throws InvalidKBException {
        Role relative = tx.putRole("relative");
        Role parent = tx.putRole("parent").sup(relative);
        Role father = tx.putRole("father").sup(parent);
        Role mother = tx.putRole("mother").sup(parent);
        Role pChild = tx.putRole("pChild").sup(relative);
        Role fmChild = tx.putRole("fChild").sup(pChild);

        //This is to bypass a specific validation rule
        tx.putRelationshipType("filler").relates(relative);

        tx.putEntityType("animal").
                plays(relative).
                plays(parent).
                plays(father).
                plays(mother).
                plays(pChild).
                plays(fmChild);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild);
        tx.putRelationshipType("fathermotherhood").sup(parenthood).relates(father).relates(mother).relates(fmChild);

        tx.commit();
    }
    @Test
    public void whenARelationTypeHasASubTypeHierarchy_EnsureThatWhenARelationTypeHasMatchingRoleTypes3() throws InvalidKBException {
        Role relative = tx.putRole("relative");
        Role parent = tx.putRole("parent").sup(relative);
        Role father = tx.putRole("father").sup(parent);
        Role pChild = tx.putRole("pChild").sup(relative);
        Role fChild = tx.putRole("fChild").sup(pChild);

        tx.putEntityType("animal").
                plays(relative).
                plays(parent).
                plays(father).
                plays(pChild).
                plays(fChild);

        RelationType parentrelativehood = tx.putRelationshipType("parentrelativehood").
                relates(relative).relates(parent).relates(pChild);
        tx.putRelationshipType("fatherhood").sup(parentrelativehood).
                relates(father).relates(fChild);

        tx.commit();
    }

    @Test
    public void whenCreatingRelationWithSubTypeHierarchyAndNoMatchingRoleTypeHierarchy_Throw1() throws InvalidKBException {
        Role pChild = tx.putRole("pChild");
        Role fChild = tx.putRole("fChild").sup(pChild);
        Role parent = tx.putRole("parent");
        Role father = tx.putRole("father").sup(parent);
        Role inContext = tx.putRole("in-context");

        tx.putEntityType("animal").plays(parent).plays(father).plays(pChild).plays(fChild);
        tx.putEntityType("context").plays(inContext);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild);
        RelationType fatherhood = tx.putRelationshipType("fatherhood").sup(parenthood).relates(father).relates(fChild).relates(inContext);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.label(), fatherhood.label(), "super", "super", parenthood.label()));

        tx.commit();
    }

    @Test
    public void whenCreatingRelationWithSubTypeHierarchyAndNoMatchingRoleTypeHierarchy_Throw2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role father = tx.putRole("father").sup(parent);
        Role pChild = tx.putRole("pChild");
        Role fChild = tx.putRole("fChild").sup(pChild);
        Role inContext = tx.putRole("in-context");

        tx.putEntityType("animal").plays(parent).plays(father).plays(pChild).plays(fChild);
        tx.putEntityType("context").plays(inContext);

        RelationType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild).relates(inContext);
        RelationType fatherhood = tx.putRelationshipType("fatherhood").sup(parenthood).relates(father).relates(fChild);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.label(), parenthood.label(), "sub", "sub", fatherhood.label()));

        tx.commit();
    }

    @Test
    public void checkRoleTypeValidSuperOfSelfTypeWhenLinkedToRelationsWhichAreSubsOfEachOther() throws InvalidKBException {
        Role insurer = tx.putRole("insurer");
        Role monoline = tx.putRole("monoline").sup(insurer);
        Role insured = tx.putRole("insured");
        RelationType insure = tx.putRelationshipType("insure").relates(insurer).relates(insured);
        tx.putRelationshipType("monoline-insure").relates(monoline).relates(insured).sup(insure);
        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsNotPlayed_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationType relationshipType = tx.putRelationshipType("my-relation").relates(role1).relates(role2);

        Thing thing = tx.putEntityType("my-entity").plays(role1).create();

        relationshipType.create().assign(role1, thing);

        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedTwice_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationType relationshipType = tx.putRelationshipType("my-relationship").relates(role1).relates(role2);

        EntityType entityType = tx.putEntityType("my-entity").plays(role1);
        Thing thing1 = entityType.create();
        Thing thing2 = entityType.create();

        Relation relationship = relationshipType.create();
        relationship.assign(role1, thing1);
        relationship.assign(role1, thing2);

        assertThat(relationship.rolePlayers(role1).collect(toSet()), hasItems(thing1, thing2));

        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedAZillionTimes_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationType relationshipType = tx.putRelationshipType("my-relationship").relates(role1).relates(role2);

        EntityType entityType = tx.putEntityType("my-entity").plays(role1);

        Relation relationship = relationshipType.create();

        Set<Thing> things = new HashSet<>();

        int oneZillion = 100;
        for (int i = 0 ; i < oneZillion; i ++) {
            Thing thing = entityType.create();
            things.add(thing);
            relationship.assign(role1, thing);
        }

        assertEquals(things, relationship.rolePlayers(role1).collect(toSet()));

        tx.commit();
    }

    @Test
    public void whenARelationTypeHasOnlyOneRole_TheGraphIsValid() {
        Role role = tx.putRole("role-1");
        tx.putRelationshipType("my-relation").relates(role);

        tx.commit();
    }


    @Test
    public void whenRemovingInvalidRolePlayers_EnsureValidationPasses(){
        Role chased = tx.putRole("chased");
        Role chaser = tx.putRole("chaser");
        RelationType chases = tx.putRelationshipType("chases").relates(chased).relates(chaser);

        EntityType puppy = tx.putEntityType("puppy").plays(chaser);
        Entity dunstan = puppy.create();

        Relation rel = chases.create();
        rel.assign(chaser, dunstan);
        rel.assign(chased, dunstan).unassign(chased, dunstan);

        tx.commit();
    }

}