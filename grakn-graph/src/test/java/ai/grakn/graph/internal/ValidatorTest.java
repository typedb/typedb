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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.util.ErrorMessage;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ValidatorTest extends GraphTestBase{

    @Test
    public void whenCommittingGraphWhichFollowsValidationRules_Commit(){
        //Actual Concepts To Appear Linked In Graph
        RelationType cast = graknGraph.putRelationType("Cast");
        Role feature = graknGraph.putRoleType("Feature");
        Role actor = graknGraph.putRoleType("Actor");
        EntityType movie = graknGraph.putEntityType("Movie");
        EntityType person = graknGraph.putEntityType("Person");
        Thing pacino = person.addEntity();
        Thing godfather = movie.addEntity();
        EntityType genre = graknGraph.putEntityType("Genre");
        Role movieOfGenre = graknGraph.putRoleType("Movie of Genre");
        Role movieGenre = graknGraph.putRoleType("Movie Genre");
        Thing crime = genre.addEntity();
        RelationType movieHasGenre = graknGraph.putRelationType("Movie Has Genre");

        //Construction
        cast.relates(feature);
        cast.relates(actor);

        cast.addRelation().
                addRolePlayer(feature, godfather).addRolePlayer(actor, pacino);

        movieHasGenre.addRelation().
                addRolePlayer(movieOfGenre, godfather).addRolePlayer(movieGenre, crime);

        movieHasGenre.relates(movieOfGenre);
        movieHasGenre.relates(movieGenre);

        movie.plays(movieOfGenre);
        person.plays(actor);
        movie.plays(feature);
        genre.plays(movieGenre);

        graknGraph.commit();
    }

    @Test
    public void whenCommittingRelationWithoutSpecifyingOntology_ThrowOnCommit(){
        EntityType fakeType = graknGraph.putEntityType("Fake Concept");
        RelationType relationType = graknGraph.putRelationType("kicks");
        Role kicker = graknGraph.putRoleType("kicker");
        Role kickee = graknGraph.putRoleType("kickee");
        Thing kyle = fakeType.addEntity();
        Thing icke = fakeType.addEntity();

        relationType.addRelation().addRolePlayer(kicker, kyle).addRolePlayer(kickee, icke);

        String error1 = ErrorMessage.VALIDATION_CASTING.getMessage(kyle.type().getLabel(), kyle.getId(), kicker.getLabel());
        String error2 = ErrorMessage.VALIDATION_CASTING.getMessage(icke.type().getLabel(), icke.getId(), kickee.getLabel());

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(allOf(containsString(error1), containsString(error2)));

        graknGraph.commit();
    }

    @Test
    public void whenCommittingNonAbstractRoleTypeNotLinkedToAnyRelationType_Throw(){
        Role alone = graknGraph.putRoleType("alone");

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(alone.getLabel())));

        graknGraph.commit();
    }

    @Test
    public void whenCommittingNonAbstractRelationTypeNotLinkedToAnyRoleType_Throw(){
        RelationType alone = graknGraph.putRelationType("alone");

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(containsString(ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(alone.getLabel())));

        graknGraph.commit();
    }

    @Test
    public void whenDeletingRelations_EnsureGraphRemainsValid() throws InvalidGraphException {
        // ontology
        EntityType person = graknGraph.putEntityType("person");
        EntityType movie = graknGraph.putEntityType("movie");
        RelationType cast = graknGraph.putRelationType("cast");
        Role feature = graknGraph.putRoleType("feature");
        Role actor = graknGraph.putRoleType("actor");
        cast.relates(feature).relates(actor);
        person.plays(actor);
        movie.plays(feature);

        // add a single movie
        Thing godfather = movie.addEntity();

        // add many random actors
        int n = 100;
        for (int i=0; i < n; i++) {
            Thing newPerson = person.addEntity();
            cast.addRelation().
                    addRolePlayer(actor, newPerson).addRolePlayer(feature, godfather);
        }

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        // now try to delete all assertions and then the movie
        godfather = graknGraph.getEntityType("movie").instances().iterator().next();
        Collection<Relation> assertions = godfather.relations();
        Set<ConceptId> assertionIds = new HashSet<>();

        for (Relation a : assertions) {
            assertionIds.add(a.getId());
            a.delete();
        }
        godfather.delete();

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        assertionIds.forEach(id -> assertNull(graknGraph.getConcept(id)));

        // assert the movie is gone
        assertNull(graknGraph.getEntityType("godfather"));
    }

    @Test
    public void whenManuallyCreatingCorrectBinaryRelation_Commit() throws InvalidGraphException {
        Role characterBeingPlayed = graknGraph.putRoleType("Character being played");
        Role personPlayingCharacter = graknGraph.putRoleType("Person Playing Char");
        RelationType playsChar = graknGraph.putRelationType("Plays Char").relates(characterBeingPlayed).relates(personPlayingCharacter);

        EntityType person = graknGraph.putEntityType("person").plays(characterBeingPlayed).plays(personPlayingCharacter);
        EntityType character = graknGraph.putEntityType("character").plays(characterBeingPlayed);

        Entity matt = person.addEntity();
        Entity walker = character.addEntity();

        playsChar.addRelation().
                addRolePlayer(personPlayingCharacter, matt).
                addRolePlayer(characterBeingPlayed, walker);

        graknGraph.commit();
    }

    /*------------------------------- Entity Type to Role Type Validation (Schema) -----------------------------------*/
    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureEntityTypesPlayAllRolesExplicitly1() throws InvalidGraphException {
        Role relative = graknGraph.putRoleType("relative");
        Role parent = graknGraph.putRoleType("parent").sup(relative);
        Role father = graknGraph.putRoleType("father").sup(parent);
        Role mother = graknGraph.putRoleType("mother").sup(parent);

        EntityType person = graknGraph.putEntityType("person").plays(relative).plays(parent);
        graknGraph.putEntityType("man").superType(person).plays(father);
        graknGraph.putEntityType("woman").superType(person).plays(mother);

        Role child = graknGraph.putRoleType("child");

        //Padding to make it valid
        graknGraph.putRelationType("filler").relates(parent).relates(child).relates(father).relates(relative).relates(mother);

        graknGraph.commit();
    }

    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureEntityTypesPlayAllRolesExplicitly2() throws InvalidGraphException {
        Role parent = graknGraph.putRoleType("parent");
        Role child = graknGraph.putRoleType("child");

        EntityType company = graknGraph.putEntityType("company").plays(parent);
        graknGraph.putEntityType("companySub").superType(company).plays(child);
        graknGraph.putEntityType("person").plays(parent).plays(child);

        //Padding to make it valid
        graknGraph.putRelationType("filler").relates(parent).relates(child);

        graknGraph.commit();
    }
    /*-------------------------------- Entity Type to Role Type Validation (Data) ------------------------------------*/

    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureInstancesCanPlayRelevantRoles1() throws InvalidGraphException {
        Role parent = graknGraph.putRoleType("parent");
        Role child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(parent).plays(child);
        EntityType man = graknGraph.putEntityType("man").superType(person);
        EntityType oneEyedMan = graknGraph.putEntityType("oneEyedMan").superType(man);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = oneEyedMan.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        graknGraph.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchy_EnsureInstancesCanPlayRelevantRoles2() throws InvalidGraphException {
        Role parent = graknGraph.putRoleType("parent");
        Role child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(parent).plays(child);
        EntityType company = graknGraph.putEntityType("company").plays(parent);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = company.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        graknGraph.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw1() throws InvalidGraphException {
        Role parent = graknGraph.putRoleType("parent");
        Role child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(parent).plays(child);
        EntityType man = graknGraph.putEntityType("man");

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = man.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(man.getLabel(), x.getId(), parent.getLabel()));

        graknGraph.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw2() throws InvalidGraphException {
        Role parent = graknGraph.putRoleType("parent");
        Role child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(child);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = person.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.getLabel(), x.getId(), parent.getLabel()));

        graknGraph.commit();
    }
    @Test
    public void whenCommittingWithRoleTypeHierarchyAndInstancesCannotPlayRolesExplicitly_Throw3() throws InvalidGraphException {
        Role parent = graknGraph.putRoleType("parent");
        Role child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(child);
        graknGraph.putEntityType("man").plays(child);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = person.addEntity();
        Entity y = person.addEntity();
        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.getLabel(), x.getId(), parent.getLabel()));

        graknGraph.commit();
    }

    /*------------------------------- Relation Type to Role Type Validation (Schema) ---------------------------------*/
    @Test
    public void whenARelationTypeHasASubTypeHierarchy_EnsureThatWhenARelationTypeHasMatchingRoleTypes1() throws InvalidGraphException {
        Role relative = graknGraph.putRoleType("relative");
        Role parent = graknGraph.putRoleType("parent").sup(relative);
        Role father = graknGraph.putRoleType("father").sup(parent);
        Role mother = graknGraph.putRoleType("mother").sup(parent);
        Role pChild = graknGraph.putRoleType("pChild").sup(relative);
        Role fChild = graknGraph.putRoleType("fChild").sup(pChild);
        Role mChild = graknGraph.putRoleType("mChild").sup(pChild);

        //This is to bypass a specific validation rule
        graknGraph.putRelationType("filler").relates(relative);

        graknGraph.putEntityType("animal").
                plays(relative).
                plays(parent).
                plays(father).
                plays(mother).
                plays(pChild).
                plays(fChild).
                plays(mChild);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(pChild);
        graknGraph.putRelationType("fatherhood").superType(parenthood).relates(father).relates(fChild);
        graknGraph.putRelationType("motherhood").superType(parenthood).relates(mother).relates(mChild);

        graknGraph.commit();
    }

    @Test
    public void whenARelationTypeHasASubTypeHierarchy_EnsureThatWhenARelationTypeHasMatchingRoleTypes2() throws InvalidGraphException {
        Role relative = graknGraph.putRoleType("relative");
        Role parent = graknGraph.putRoleType("parent").sup(relative);
        Role father = graknGraph.putRoleType("father").sup(parent);
        Role mother = graknGraph.putRoleType("mother").sup(parent);
        Role pChild = graknGraph.putRoleType("pChild").sup(relative);
        Role fmChild = graknGraph.putRoleType("fChild").sup(pChild);

        //This is to bypass a specific validation rule
        graknGraph.putRelationType("filler").relates(relative);

        graknGraph.putEntityType("animal").
                plays(relative).
                plays(parent).
                plays(father).
                plays(mother).
                plays(pChild).
                plays(fmChild);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(pChild);
        graknGraph.putRelationType("fathermotherhood").superType(parenthood).relates(father).relates(mother).relates(fmChild);

        graknGraph.commit();
    }
    @Test
    public void whenARelationTypeHasASubTypeHierarchy_EnsureThatWhenARelationTypeHasMatchingRoleTypes3() throws InvalidGraphException {
        Role relative = graknGraph.putRoleType("relative");
        Role parent = graknGraph.putRoleType("parent").sup(relative);
        Role father = graknGraph.putRoleType("father").sup(parent);
        Role pChild = graknGraph.putRoleType("pChild").sup(relative);
        Role fChild = graknGraph.putRoleType("fChild").sup(pChild);

        graknGraph.putEntityType("animal").
                plays(relative).
                plays(parent).
                plays(father).
                plays(pChild).
                plays(fChild);

        RelationType parentrelativehood = graknGraph.putRelationType("parentrelativehood").
                relates(relative).relates(parent).relates(pChild);
        graknGraph.putRelationType("fatherhood").superType(parentrelativehood).
                relates(father).relates(fChild);

        graknGraph.commit();
    }

    @Test
    public void whenCreatingRelationWithSubTypeHierarchyAndNoMatchingRoleTypeHierarchy_Throw1() throws InvalidGraphException {
        Role pChild = graknGraph.putRoleType("pChild");
        Role fChild = graknGraph.putRoleType("fChild").sup(pChild);
        Role parent = graknGraph.putRoleType("parent");
        Role father = graknGraph.putRoleType("father").sup(parent);
        Role inContext = graknGraph.putRoleType("in-context");

        graknGraph.putEntityType("animal").plays(parent).plays(father).plays(pChild).plays(fChild);
        graknGraph.putEntityType("context").plays(inContext);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(pChild);
        RelationType fatherhood = graknGraph.putRelationType("fatherhood").superType(parenthood).relates(father).relates(fChild).relates(inContext);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.getLabel(), fatherhood.getLabel(), "super", "super", parenthood.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void whenCreatingRelationWithSubTypeHierarchyAndNoMatchingRoleTypeHierarchy_Throw2() throws InvalidGraphException {
        Role parent = graknGraph.putRoleType("parent");
        Role father = graknGraph.putRoleType("father").sup(parent);
        Role pChild = graknGraph.putRoleType("pChild");
        Role fChild = graknGraph.putRoleType("fChild").sup(pChild);
        Role inContext = graknGraph.putRoleType("in-context");

        graknGraph.putEntityType("animal").plays(parent).plays(father).plays(pChild).plays(fChild);
        graknGraph.putEntityType("context").plays(inContext);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(pChild).relates(inContext);
        RelationType fatherhood = graknGraph.putRelationType("fatherhood").superType(parenthood).relates(father).relates(fChild);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.getLabel(), parenthood.getLabel(), "sub", "sub", fatherhood.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void checkRoleTypeValidSuperOfSelfTypeWhenLinkedToRelationsWhichAreSubsOfEachOther() throws InvalidGraphException {
        Role insurer = graknGraph.putRoleType("insurer");
        Role monoline = graknGraph.putRoleType("monoline").sup(insurer);
        Role insured = graknGraph.putRoleType("insured");
        RelationType insure = graknGraph.putRelationType("insure").relates(insurer).relates(insured);
        graknGraph.putRelationType("monoline-insure").relates(monoline).relates(insured).superType(insure);
        graknGraph.commit();
    }

    @Test
    public void whenARoleInARelationIsNotPlayed_TheGraphIsValid() {
        Role role1 = graknGraph.putRoleType("role-1");
        Role role2 = graknGraph.putRoleType("role-2");
        RelationType relationType = graknGraph.putRelationType("my-relation").relates(role1).relates(role2);

        Thing thing = graknGraph.putEntityType("my-entity").plays(role1).addEntity();

        relationType.addRelation().addRolePlayer(role1, thing);

        graknGraph.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedTwice_TheGraphIsValid() {
        Role role1 = graknGraph.putRoleType("role-1");
        Role role2 = graknGraph.putRoleType("role-2");
        RelationType relationType = graknGraph.putRelationType("my-relation").relates(role1).relates(role2);

        EntityType entityType = graknGraph.putEntityType("my-entity").plays(role1);
        Thing thing1 = entityType.addEntity();
        Thing thing2 = entityType.addEntity();

        Relation relation = relationType.addRelation();
        relation.addRolePlayer(role1, thing1);
        relation.addRolePlayer(role1, thing2);

        assertThat(relation.rolePlayers(role1), hasItems(thing1, thing2));

        graknGraph.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedAZillionTimes_TheGraphIsValid() {
        Role role1 = graknGraph.putRoleType("role-1");
        Role role2 = graknGraph.putRoleType("role-2");
        RelationType relationType = graknGraph.putRelationType("my-relation").relates(role1).relates(role2);

        EntityType entityType = graknGraph.putEntityType("my-entity").plays(role1);

        Relation relation = relationType.addRelation();

        Set<Thing> things = new HashSet<>();

        int oneZillion = 100;
        for (int i = 0 ; i < oneZillion; i ++) {
            Thing thing = entityType.addEntity();
            things.add(thing);
            relation.addRolePlayer(role1, thing);
        }

        assertEquals(things, relation.rolePlayers(role1));

        graknGraph.commit();
    }

    @Test
    public void whenARelationTypeHasOnlyOneRole_TheGraphIsValid() {
        Role role = graknGraph.putRoleType("role-1");
        graknGraph.putRelationType("my-relation").relates(role);

        graknGraph.commit();
    }

}