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

package ai.grakn.kb.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.util.ErrorMessage;
import org.junit.Test;

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

public class ValidatorTest extends TxTestBase {


    @Test
    public void whenCreatingAbstractRelationshipWithSubType_EnsureValidationRuleForMatchingSubRolesIsSkipped(){
        Role role1 = tx.putRole("my role");
        Role role2 = tx.putRole("my role 2");

        RelationshipType abstractRelationType = tx.putRelationshipType("my abstract relation type").
                relates(role1).
                setAbstract(true);
        tx.putRelationshipType("my relation type").
                sup(abstractRelationType).
                relates(role2);

        tx.commit();
    }

    @Test
    public void whenCommittingGraphWhichFollowsValidationRules_Commit(){
        RelationshipType cast = tx.putRelationshipType("Cast");
        Role feature = tx.putRole("Feature");
        Role actor = tx.putRole("Actor");
        EntityType movie = tx.putEntityType("Movie");
        EntityType person = tx.putEntityType("Person");
        Thing pacino = person.addEntity();
        Thing godfather = movie.addEntity();
        EntityType genre = tx.putEntityType("Genre");
        Role movieOfGenre = tx.putRole("Movie of Genre");
        Role movieGenre = tx.putRole("Movie Genre");
        Thing crime = genre.addEntity();
        RelationshipType movieHasGenre = tx.putRelationshipType("Movie Has Genre");

        //Construction
        cast.relates(feature);
        cast.relates(actor);

        cast.addRelationship().
                addRolePlayer(feature, godfather).addRolePlayer(actor, pacino);

        movieHasGenre.addRelationship().
                addRolePlayer(movieOfGenre, godfather).addRolePlayer(movieGenre, crime);

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
        RelationshipType relationshipType = tx.putRelationshipType("kicks");
        Role kicker = tx.putRole("kicker");
        Role kickee = tx.putRole("kickee");
        Thing kyle = fakeType.addEntity();
        Thing icke = fakeType.addEntity();

        relationshipType.addRelationship().addRolePlayer(kicker, kyle).addRolePlayer(kickee, icke);

        String error1 = ErrorMessage.VALIDATION_CASTING.getMessage(kyle.type().getLabel(), kyle.getId(), kicker.getLabel());
        String error2 = ErrorMessage.VALIDATION_CASTING.getMessage(icke.type().getLabel(), icke.getId(), kickee.getLabel());

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(allOf(containsString(error1), containsString(error2)));

        tx.commit();
    }

    @Test
    public void whenCommittingNonAbstractRoleTypeNotLinkedToAnyRelationType_Throw(){
        Role alone = tx.putRole("alone");

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(alone.getLabel())));

        tx.commit();
    }

    @Test
    public void whenCommittingNonAbstractRelationTypeNotLinkedToAnyRoleType_Throw(){
        RelationshipType alone = tx.putRelationshipType("alone");

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(alone.getLabel())));

        tx.commit();
    }

    @Test
    public void whenCreatingRelationWithoutLinkingRelates_Throw(){
        Role hunter = tx.putRole("hunter");
        Role monster = tx.putRole("monster");
        EntityType stuff = tx.putEntityType("Stuff").plays(hunter).plays(monster);
        RelationshipType kills = tx.putRelationshipType("kills").relates(hunter);

        Entity myHunter = stuff.addEntity();
        Entity myMonster = stuff.addEntity();

        Relationship relation = kills.addRelationship().addRolePlayer(hunter, myHunter).addRolePlayer(monster, myMonster);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relation.getId(), monster.getLabel(), kills.getLabel())));

        tx.commit();
    }

    @Test
    public void whenDeletingRelations_EnsureGraphRemainsValid() throws InvalidKBException {
        // schema
        EntityType person = tx.putEntityType("person");
        EntityType movie = tx.putEntityType("movie");
        RelationshipType cast = tx.putRelationshipType("cast");
        Role feature = tx.putRole("feature");
        Role actor = tx.putRole("actor");
        cast.relates(feature).relates(actor);
        person.plays(actor);
        movie.plays(feature);

        // add a single movie
        Thing godfather = movie.addEntity();

        // add many random actors
        int n = 100;
        for (int i=0; i < n; i++) {
            Thing newPerson = person.addEntity();
            cast.addRelationship().
                    addRolePlayer(actor, newPerson).addRolePlayer(feature, godfather);
        }

        tx.commit();
        tx = (GraknTxAbstract<?>) Grakn.session(Grakn.IN_MEMORY, tx.getKeyspace()).open(GraknTxType.WRITE);

        // now try to delete all assertions and then the movie
        godfather = tx.getEntityType("movie").instances().iterator().next();
        Collection<Relationship> assertions = godfather.relationships().collect(Collectors.toSet());
        Set<ConceptId> assertionIds = new HashSet<>();

        for (Relationship a : assertions) {
            assertionIds.add(a.getId());
            a.delete();
        }
        godfather.delete();

        tx.commit();
        tx = (GraknTxAbstract<?>) Grakn.session(Grakn.IN_MEMORY, tx.getKeyspace()).open(GraknTxType.WRITE);

        assertionIds.forEach(id -> assertNull(tx.getConcept(id)));

        // assert the movie is gone
        assertNull(tx.getEntityType("godfather"));
    }

    @Test
    public void whenManuallyCreatingCorrectBinaryRelation_Commit() throws InvalidKBException {
        Role characterBeingPlayed = tx.putRole("Character being played");
        Role personPlayingCharacter = tx.putRole("Person Playing Char");
        RelationshipType playsChar = tx.putRelationshipType("Plays Char").relates(characterBeingPlayed).relates(personPlayingCharacter);

        EntityType person = tx.putEntityType("person").plays(characterBeingPlayed).plays(personPlayingCharacter);
        EntityType character = tx.putEntityType("character").plays(characterBeingPlayed);

        Entity matt = person.addEntity();
        Entity walker = character.addEntity();

        playsChar.addRelationship().
                addRolePlayer(personPlayingCharacter, matt).
                addRolePlayer(characterBeingPlayed, walker);

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

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = oneEyedMan.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelationship().addRolePlayer(parent, x).addRolePlayer(child, y);

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureInstancesCanPlayRelevantRoles2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(parent).plays(child);
        EntityType company = tx.putEntityType("company").plays(parent);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = company.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelationship().addRolePlayer(parent, x).addRolePlayer(child, y);

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw1() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(parent).plays(child);
        EntityType man = tx.putEntityType("man");

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = man.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelationship().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(man.getLabel(), x.getId(), parent.getLabel()));

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(child);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = person.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelationship().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.getLabel(), x.getId(), parent.getLabel()));

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw3() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").plays(child);
        tx.putEntityType("man").plays(child);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(child);

        Entity x = person.addEntity();
        Entity y = person.addEntity();
        parenthood.addRelationship().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.getLabel(), x.getId(), parent.getLabel()));

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

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild);
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

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild);
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

        RelationshipType parentrelativehood = tx.putRelationshipType("parentrelativehood").
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

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild);
        RelationshipType fatherhood = tx.putRelationshipType("fatherhood").sup(parenthood).relates(father).relates(fChild).relates(inContext);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.getLabel(), fatherhood.getLabel(), "super", "super", parenthood.getLabel()));

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

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relates(parent).relates(pChild).relates(inContext);
        RelationshipType fatherhood = tx.putRelationshipType("fatherhood").sup(parenthood).relates(father).relates(fChild);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.getLabel(), parenthood.getLabel(), "sub", "sub", fatherhood.getLabel()));

        tx.commit();
    }

    @Test
    public void checkRoleTypeValidSuperOfSelfTypeWhenLinkedToRelationsWhichAreSubsOfEachOther() throws InvalidKBException {
        Role insurer = tx.putRole("insurer");
        Role monoline = tx.putRole("monoline").sup(insurer);
        Role insured = tx.putRole("insured");
        RelationshipType insure = tx.putRelationshipType("insure").relates(insurer).relates(insured);
        tx.putRelationshipType("monoline-insure").relates(monoline).relates(insured).sup(insure);
        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsNotPlayed_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationshipType relationshipType = tx.putRelationshipType("my-relation").relates(role1).relates(role2);

        Thing thing = tx.putEntityType("my-entity").plays(role1).addEntity();

        relationshipType.addRelationship().addRolePlayer(role1, thing);

        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedTwice_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationshipType relationshipType = tx.putRelationshipType("my-relationship").relates(role1).relates(role2);

        EntityType entityType = tx.putEntityType("my-entity").plays(role1);
        Thing thing1 = entityType.addEntity();
        Thing thing2 = entityType.addEntity();

        Relationship relationship = relationshipType.addRelationship();
        relationship.addRolePlayer(role1, thing1);
        relationship.addRolePlayer(role1, thing2);

        assertThat(relationship.rolePlayers(role1).collect(toSet()), hasItems(thing1, thing2));

        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedAZillionTimes_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationshipType relationshipType = tx.putRelationshipType("my-relationship").relates(role1).relates(role2);

        EntityType entityType = tx.putEntityType("my-entity").plays(role1);

        Relationship relationship = relationshipType.addRelationship();

        Set<Thing> things = new HashSet<>();

        int oneZillion = 100;
        for (int i = 0 ; i < oneZillion; i ++) {
            Thing thing = entityType.addEntity();
            things.add(thing);
            relationship.addRolePlayer(role1, thing);
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

}