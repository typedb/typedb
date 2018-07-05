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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
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
import ai.grakn.factory.EmbeddedGraknSession;
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
                relate(role1).
                isAbstract(true);
        tx.putRelationshipType("my relation type").
                sup(abstractRelationType).
                relate(role2);

        tx.commit();
    }

    @Test
    public void whenCommittingGraphWhichFollowsValidationRules_Commit(){
        RelationshipType cast = tx.putRelationshipType("Cast");
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
        RelationshipType movieHasGenre = tx.putRelationshipType("Movie Has Genre");

        //Construction
        cast.relate(feature);
        cast.relate(actor);

        cast.create().
                assign(feature, godfather).assign(actor, pacino);

        movieHasGenre.create().
                assign(movieOfGenre, godfather).assign(movieGenre, crime);

        movieHasGenre.relate(movieOfGenre);
        movieHasGenre.relate(movieGenre);

        movie.play(movieOfGenre);
        person.play(actor);
        movie.play(feature);
        genre.play(movieGenre);

        tx.commit();
    }

    @Test
    public void whenCommittingRelationWithoutSpecifyingSchema_ThrowOnCommit(){
        EntityType fakeType = tx.putEntityType("Fake Concept");
        RelationshipType relationshipType = tx.putRelationshipType("kicks");
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
        RelationshipType alone = tx.putRelationshipType("alone");

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(alone.label())));

        tx.commit();
    }

    @Test
    public void whenCreatingRelationWithoutLinkingRelates_Throw(){
        Role hunter = tx.putRole("hunter");
        Role monster = tx.putRole("monster");
        EntityType stuff = tx.putEntityType("Stuff").play(hunter).play(monster);
        RelationshipType kills = tx.putRelationshipType("kills").relate(hunter);

        Entity myHunter = stuff.create();
        Entity myMonster = stuff.create();

        Relationship relation = kills.create().assign(hunter, myHunter).assign(monster, myMonster);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relation.id(), monster.label(), kills.label())));

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
        cast.relate(feature).relate(actor);
        person.play(actor);
        movie.play(feature);

        // add a single movie
        Thing godfather = movie.create();

        // add many random actors
        int n = 100;
        for (int i=0; i < n; i++) {
            Thing newPerson = person.create();
            cast.create().
                    assign(actor, newPerson).assign(feature, godfather);
        }

        tx.commit();
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).transaction(GraknTxType.WRITE);

        // now try to delete all assertions and then the movie
        godfather = tx.getEntityType("movie").instances().iterator().next();
        Collection<Relationship> assertions = godfather.relationships().collect(Collectors.toSet());
        Set<ConceptId> assertionIds = new HashSet<>();

        for (Relationship a : assertions) {
            assertionIds.add(a.id());
            a.delete();
        }
        godfather.delete();

        tx.commit();
        tx = EmbeddedGraknSession.create(tx.keyspace(), Grakn.IN_MEMORY).transaction(GraknTxType.WRITE);

        assertionIds.forEach(id -> assertNull(tx.getConcept(id)));

        // assert the movie is gone
        assertNull(tx.getEntityType("godfather"));
    }

    @Test
    public void whenManuallyCreatingCorrectBinaryRelation_Commit() throws InvalidKBException {
        Role characterBeingPlayed = tx.putRole("Character being played");
        Role personPlayingCharacter = tx.putRole("Person Playing Char");
        RelationshipType playsChar = tx.putRelationshipType("Plays Char").relate(characterBeingPlayed).relate(personPlayingCharacter);

        EntityType person = tx.putEntityType("person").play(characterBeingPlayed).play(personPlayingCharacter);
        EntityType character = tx.putEntityType("character").play(characterBeingPlayed);

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

        EntityType person = tx.putEntityType("person").play(relative).play(parent);
        tx.putEntityType("man").sup(person).play(father);
        tx.putEntityType("woman").sup(person).play(mother);

        Role child = tx.putRole("child");

        //Padding to make it valid
        tx.putRelationshipType("filler").relate(parent).relate(child).relate(father).relate(relative).relate(mother);

        tx.commit();
    }

    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureEntityTypesPlayAllRolesExplicitly2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType company = tx.putEntityType("company").play(parent);
        tx.putEntityType("companySub").sup(company).play(child);
        tx.putEntityType("person").play(parent).play(child);

        //Padding to make it valid
        tx.putRelationshipType("filler").relate(parent).relate(child);

        tx.commit();
    }
    /*-------------------------------- Entity Type to Role Type Validation (Data) ------------------------------------*/

    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureInstancesCanPlayRelevantRoles1() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").play(parent).play(child);
        EntityType man = tx.putEntityType("man").sup(person);
        EntityType oneEyedMan = tx.putEntityType("oneEyedMan").sup(man);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(child);

        Entity x = oneEyedMan.create();
        Entity y = person.create();

        parenthood.create().assign(parent, x).assign(child, y);

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureInstancesCanPlayRelevantRoles2() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").play(parent).play(child);
        EntityType company = tx.putEntityType("company").play(parent);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(child);

        Entity x = company.create();
        Entity y = person.create();

        parenthood.create().assign(parent, x).assign(child, y);

        tx.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw1() throws InvalidKBException {
        Role parent = tx.putRole("parent");
        Role child = tx.putRole("child");

        EntityType person = tx.putEntityType("person").play(parent).play(child);
        EntityType man = tx.putEntityType("man");

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(child);

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

        EntityType person = tx.putEntityType("person").play(child);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(child);

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

        EntityType person = tx.putEntityType("person").play(child);
        tx.putEntityType("man").play(child);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(child);

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
        tx.putRelationshipType("filler").relate(relative);

        tx.putEntityType("animal").
                play(relative).
                play(parent).
                play(father).
                play(mother).
                play(pChild).
                play(fChild).
                play(mChild);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(pChild);
        tx.putRelationshipType("fatherhood").sup(parenthood).relate(father).relate(fChild);
        tx.putRelationshipType("motherhood").sup(parenthood).relate(mother).relate(mChild);

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
        tx.putRelationshipType("filler").relate(relative);

        tx.putEntityType("animal").
                play(relative).
                play(parent).
                play(father).
                play(mother).
                play(pChild).
                play(fmChild);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(pChild);
        tx.putRelationshipType("fathermotherhood").sup(parenthood).relate(father).relate(mother).relate(fmChild);

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
                play(relative).
                play(parent).
                play(father).
                play(pChild).
                play(fChild);

        RelationshipType parentrelativehood = tx.putRelationshipType("parentrelativehood").
                relate(relative).relate(parent).relate(pChild);
        tx.putRelationshipType("fatherhood").sup(parentrelativehood).
                relate(father).relate(fChild);

        tx.commit();
    }

    @Test
    public void whenCreatingRelationWithSubTypeHierarchyAndNoMatchingRoleTypeHierarchy_Throw1() throws InvalidKBException {
        Role pChild = tx.putRole("pChild");
        Role fChild = tx.putRole("fChild").sup(pChild);
        Role parent = tx.putRole("parent");
        Role father = tx.putRole("father").sup(parent);
        Role inContext = tx.putRole("in-context");

        tx.putEntityType("animal").play(parent).play(father).play(pChild).play(fChild);
        tx.putEntityType("context").play(inContext);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(pChild);
        RelationshipType fatherhood = tx.putRelationshipType("fatherhood").sup(parenthood).relate(father).relate(fChild).relate(inContext);

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

        tx.putEntityType("animal").play(parent).play(father).play(pChild).play(fChild);
        tx.putEntityType("context").play(inContext);

        RelationshipType parenthood = tx.putRelationshipType("parenthood").relate(parent).relate(pChild).relate(inContext);
        RelationshipType fatherhood = tx.putRelationshipType("fatherhood").sup(parenthood).relate(father).relate(fChild);

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
        RelationshipType insure = tx.putRelationshipType("insure").relate(insurer).relate(insured);
        tx.putRelationshipType("monoline-insure").relate(monoline).relate(insured).sup(insure);
        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsNotPlayed_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationshipType relationshipType = tx.putRelationshipType("my-relation").relate(role1).relate(role2);

        Thing thing = tx.putEntityType("my-entity").play(role1).create();

        relationshipType.create().assign(role1, thing);

        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedTwice_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationshipType relationshipType = tx.putRelationshipType("my-relationship").relate(role1).relate(role2);

        EntityType entityType = tx.putEntityType("my-entity").play(role1);
        Thing thing1 = entityType.create();
        Thing thing2 = entityType.create();

        Relationship relationship = relationshipType.create();
        relationship.assign(role1, thing1);
        relationship.assign(role1, thing2);

        assertThat(relationship.rolePlayers(role1).collect(toSet()), hasItems(thing1, thing2));

        tx.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedAZillionTimes_TheGraphIsValid() {
        Role role1 = tx.putRole("role-1");
        Role role2 = tx.putRole("role-2");
        RelationshipType relationshipType = tx.putRelationshipType("my-relationship").relate(role1).relate(role2);

        EntityType entityType = tx.putEntityType("my-entity").play(role1);

        Relationship relationship = relationshipType.create();

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
        tx.putRelationshipType("my-relation").relate(role);

        tx.commit();
    }


    @Test
    public void whenRemovingInvalidRolePlayers_EnsureValidationPasses(){
        Role chased = tx.putRole("chased");
        Role chaser = tx.putRole("chaser");
        RelationshipType chases = tx.putRelationshipType("chases").relate(chased).relate(chaser);

        EntityType puppy = tx.putEntityType("puppy").play(chaser);
        Entity dunstan = puppy.create();

        Relationship rel = chases.create();
        rel.assign(chaser, dunstan);
        rel.assign(chased, dunstan).unassign(chased, dunstan);

        tx.commit();
    }

}