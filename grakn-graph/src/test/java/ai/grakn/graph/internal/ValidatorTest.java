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
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.util.ErrorMessage;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ValidatorTest extends GraphTestBase{

    @Test
    public void testGetErrorsFound() throws Exception {
        Validator validator = new Validator(null);
        assertNotNull(validator.getErrorsFound());
    }

    private boolean expectedErrorFound(Validator validator, String expectedError){
        for(String error: validator.getErrorsFound()){
            if(error.contains(expectedError)){
                return true;
            }
        }
        return false;
    }

    @Test
    public void testValidateBigTest(){
        //Actual Concepts To Appear Linked In Graph
        RelationType cast = graknGraph.putRelationType("Cast");
        RoleType feature = graknGraph.putRoleType("Feature");
        RoleType actor = graknGraph.putRoleType("Actor");
        EntityType movie = graknGraph.putEntityType("Movie");
        EntityType person = graknGraph.putEntityType("Person");
        Instance pacino = person.addEntity();
        Instance godfather = movie.addEntity();
        EntityType genre = graknGraph.putEntityType("Genre");
        RoleType movieOfGenre = graknGraph.putRoleType("Movie of Genre");
        RoleType movieGenre = graknGraph.putRoleType("Movie Genre");
        Instance crime = genre.addEntity();
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

        boolean exceptionThrown = false;
        try {
            graknGraph.validateGraph();
        } catch (GraknValidationException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);
    }

    @Test
    public void castingValidationOfRoleTypeAndPlaysEdge(){
        EntityType fakeType = graknGraph.putEntityType("Fake Concept");
        RelationType relationType = graknGraph.putRelationType("kicks");
        RoleType kicker = graknGraph.putRoleType("kicker");
        RoleType kickee = graknGraph.putRoleType("kickee");
        Instance kyle = fakeType.addEntity();
        Instance icke = fakeType.addEntity();

        RelationImpl assertion = (RelationImpl) relationType.addRelation().
                addRolePlayer(kicker, kyle).addRolePlayer(kickee, icke);

        boolean failure = false;
        try {
            graknGraph.validateGraph();
        } catch (GraknValidationException e) {
            failure = true;
        }
        assertTrue(failure);

        Validator validator = new Validator(graknGraph);
        assertFalse(validator.validate());
        assertEquals(6, validator.getErrorsFound().size());

        CastingImpl casting1 = (CastingImpl) assertion.getMappingCasting().toArray()[0];
        CastingImpl casting2 = (CastingImpl) assertion.getMappingCasting().toArray()[1];
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_CASTING.getMessage(
                casting1.getRolePlayer().type().getLabel(), casting1.getRolePlayer().getId(), casting1.getRole().getLabel())));
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_CASTING.getMessage(
                casting2.getRolePlayer().type().getLabel(), casting2.getRolePlayer().getId(), casting2.getRole().getLabel())));
    }

    @Test
    public void relatesEdgeTestFail(){
        RoleType alone = graknGraph.putRoleType("alone");
        Validator validator = new Validator(graknGraph);
        assertFalse(validator.validate());
        assertEquals(1, validator.getErrorsFound().size());
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(alone.getLabel())));
    }

    @Test
    public void relationTypeRelatesTest(){
        RelationType alone = graknGraph.putRelationType("alone");
        Validator validator = new Validator(graknGraph);
        assertFalse(validator.validate());
        assertEquals(1, validator.getErrorsFound().size());
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(alone.getLabel())));
    }

    @Test
    public void validateAssertionFail(){
        EntityType fakeType = graknGraph.putEntityType("Fake Concept");
        RelationType relationType = graknGraph.putRelationType("kicks");
        RoleType kicker = graknGraph.putRoleType("kicker");
        RoleType kickee = graknGraph.putRoleType("kickee");
        InstanceImpl kyle = (InstanceImpl) fakeType.addEntity();
        InstanceImpl icke = (InstanceImpl) fakeType.addEntity();

        relationType.addRelation().
                addRolePlayer(kicker, kyle).addRolePlayer(kickee, icke);

        Validator validator = new Validator(graknGraph);
        assertFalse(validator.validate());

        assertEquals(6, validator.getErrorsFound().size());
    }

    @Test
    public void validateCastingFail(){
        EntityType fakeType = graknGraph.putEntityType("Fake Concept");
        RelationType relationType = graknGraph.putRelationType("kicks");
        RoleType kicker = graknGraph.putRoleType("kicker");
        RoleType kickee = graknGraph.putRoleType("kickee");
        Instance kyle = fakeType.addEntity();
        Instance icke = fakeType.addEntity();

        RelationImpl assertion = (RelationImpl) relationType.addRelation().
                addRolePlayer(kicker, kyle).addRolePlayer(kickee, icke);
        CastingImpl casting = (CastingImpl) assertion.getMappingCasting().toArray()[0];
        Validator validator = new Validator(graknGraph);
        assertFalse(validator.validate());
        assertEquals(6, validator.getErrorsFound().size());
    }

    @Test
    public void validateIsAbstract(){
        EntityType x1 = graknGraph.putEntityType("x1");
        EntityType x2 = graknGraph.putEntityType("x2");
        EntityType x3 = graknGraph.putEntityType("x3");
        EntityType x4 = graknGraph.putEntityType("x4");
        Instance x5 = x1.addEntity();

        x1.setAbstract(true);
        x4.setAbstract(true);

        x4.superType(x3);

        Validator validator = new Validator(graknGraph);

        validator.validate();

        assertTrue((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x1.getLabel()))));
        assertFalse((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x2.getLabel()))));
        assertFalse((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x3.getLabel()))));
        assertFalse((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x4.getLabel()))));
    }

    @Test
    public void testValidateAfterManualAssertionDelete() throws GraknValidationException {
        // ontology
        EntityType person = graknGraph.putEntityType("person");
        EntityType movie = graknGraph.putEntityType("movie");
        RelationType cast = graknGraph.putRelationType("cast");
        RoleType feature = graknGraph.putRoleType("feature");
        RoleType actor = graknGraph.putRoleType("actor");
        cast.relates(feature).relates(actor);
        person.plays(actor);
        movie.plays(feature);

        // add a single movie
        Instance godfather = movie.addEntity();

        // add many random actors
        int n = 100;
        for (int i=0; i < n; i++) {
            Instance newPerson = person.addEntity();
            cast.addRelation().
                    addRolePlayer(actor, newPerson).addRolePlayer(feature, godfather);
        }

        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        // now try to delete all assertions and then the movie
        godfather = graknGraph.getEntityType("movie").instances().iterator().next();
        Collection<Relation> assertions = godfather.relations();
        Set<ConceptId> assertionIds = new HashSet<>();
        Set<ConceptId> castingIds = new HashSet<>();
        for (Relation a : assertions) {
            assertionIds.add(a.getId());
            ((RelationImpl) a).getMappingCasting().forEach(c -> castingIds.add(c.getId()));
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
    public void testRoleTypeCanPlayRoleIfAbstract() throws GraknValidationException {
        RoleType role1 = graknGraph.putRoleType("role1").setAbstract(true);
        RoleType role2 = graknGraph.putRoleType("role2").setAbstract(true);
        graknGraph.putEntityType("my type").plays(role1).plays(role2);
        graknGraph.commit();
    }

    @Test
    public void testNormalRelationshipWithTwoPlays() throws GraknValidationException {
        RoleType characterBeingPlayed = graknGraph.putRoleType("Character being played");
        RoleType personPlayingCharacter = graknGraph.putRoleType("Person Playing Char");
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
    public void testRoleToRolePlayersSchemaValidationValid1() throws GraknValidationException {
        RoleType relative = graknGraph.putRoleType("relative");
        RoleType parent = graknGraph.putRoleType("parent").superType(relative);
        RoleType father = graknGraph.putRoleType("father").superType(parent);
        RoleType mother = graknGraph.putRoleType("mother").superType(parent);

        EntityType person = graknGraph.putEntityType("person").plays(relative).plays(parent);
        graknGraph.putEntityType("man").superType(person).plays(father);
        graknGraph.putEntityType("woman").superType(person).plays(mother);

        RoleType child = graknGraph.putRoleType("child");

        //Padding to make it valid
        graknGraph.putRelationType("filler").relates(parent).relates(child).relates(father).relates(relative).relates(mother);

        graknGraph.commit();
    }

    @Test
    public void testRoleToRolePlayersSchemaValidationValid2() throws GraknValidationException {
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType child = graknGraph.putRoleType("child");

        EntityType company = graknGraph.putEntityType("company").plays(parent);
        graknGraph.putEntityType("companySub").superType(company).plays(child);
        graknGraph.putEntityType("person").plays(parent).plays(child);

        //Padding to make it valid
        graknGraph.putRelationType("filler").relates(parent).relates(child);

        graknGraph.commit();
    }
    /*-------------------------------- Entity Type to Role Type Validation (Data) ------------------------------------*/

    @Test
    public void testRoleToRolePlayersDataValidationValid1() throws GraknValidationException {
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType child = graknGraph.putRoleType("child");

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
    public void testRoleToRolePlayersDataValidationValid2() throws GraknValidationException {
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(parent).plays(child);
        EntityType company = graknGraph.putEntityType("company").plays(parent);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = company.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        graknGraph.commit();
    }
    @Test
    public void testRoleToRolePlayersDataValidationInvalid1() throws GraknValidationException {
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(parent).plays(child);
        EntityType man = graknGraph.putEntityType("man");

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = man.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(man.getLabel(), x.getId(), parent.getLabel()));

        graknGraph.commit();
    }
    @Test
    public void testRoleToRolePlayersDataValidationInvalid2() throws GraknValidationException {
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(child);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = person.addEntity();
        Entity y = person.addEntity();

        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.getLabel(), x.getId(), parent.getLabel()));

        graknGraph.commit();
    }
    @Test
    public void testRoleToRolePlayersDataValidationInvalid3() throws GraknValidationException {
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType child = graknGraph.putRoleType("child");

        EntityType person = graknGraph.putEntityType("person").plays(child);
        EntityType man = graknGraph.putEntityType("man").plays(child);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(child);

        Entity x = person.addEntity();
        Entity y = person.addEntity();
        parenthood.addRelation().addRolePlayer(parent, x).addRolePlayer(child, y);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_CASTING.getMessage(person.getLabel(), x.getId(), parent.getLabel()));

        graknGraph.commit();
    }

    /*------------------------------- Relation Type to Role Type Validation (Schema) ---------------------------------*/
    @Test
    public void testRelationTypeToRoleTypeSchemaValidationValid1() throws GraknValidationException {
        RoleType relative = graknGraph.putRoleType("relative").setAbstract(true);
        RoleType parent = graknGraph.putRoleType("parent").superType(relative);
        RoleType father = graknGraph.putRoleType("father").superType(parent);
        RoleType mother = graknGraph.putRoleType("mother").superType(parent);
        RoleType pChild = graknGraph.putRoleType("pChild").superType(relative);
        RoleType fChild = graknGraph.putRoleType("fChild").superType(pChild);
        RoleType mChild = graknGraph.putRoleType("mChild").superType(pChild);

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
    public void testRelationTypeToRoleTypeSchemaValidationValid2() throws GraknValidationException {
        RoleType relative = graknGraph.putRoleType("relative").setAbstract(true);
        RoleType parent = graknGraph.putRoleType("parent").superType(relative);
        RoleType father = graknGraph.putRoleType("father").superType(parent);
        RoleType mother = graknGraph.putRoleType("mother").superType(parent);
        RoleType pChild = graknGraph.putRoleType("pChild").superType(relative);
        RoleType fmChild = graknGraph.putRoleType("fChild").superType(pChild);

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
    public void testRelationTypeToRoleTypeSchemaValidationValid3() throws GraknValidationException {
        RoleType relative = graknGraph.putRoleType("relative");
        RoleType parent = graknGraph.putRoleType("parent").superType(relative);
        RoleType father = graknGraph.putRoleType("father").superType(parent);
        RoleType pChild = graknGraph.putRoleType("pChild").superType(relative);
        RoleType fChild = graknGraph.putRoleType("fChild").superType(pChild);

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
    public void testRelationTypeToRoleTypeSchemaValidationInvalid1() throws GraknValidationException {
        RoleType pChild = graknGraph.putRoleType("pChild");
        RoleType fChild = graknGraph.putRoleType("fChild").superType(pChild);
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType father = graknGraph.putRoleType("father").superType(parent);
        RoleType inContext = graknGraph.putRoleType("in-context");

        graknGraph.putEntityType("animal").plays(parent).plays(father).plays(pChild).plays(fChild);
        graknGraph.putEntityType("context").plays(inContext);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(pChild);
        RelationType fatherhood = graknGraph.putRelationType("fatherhood").superType(parenthood).relates(father).relates(fChild).relates(inContext);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.getLabel(), fatherhood.getLabel(), "super", "super", parenthood.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void testRelationTypeToRoleTypeSchemaValidationInvalid2() throws GraknValidationException {
        RoleType parent = graknGraph.putRoleType("parent");
        RoleType father = graknGraph.putRoleType("father").superType(parent);
        RoleType pChild = graknGraph.putRoleType("pChild");
        RoleType fChild = graknGraph.putRoleType("fChild").superType(pChild);
        RoleType inContext = graknGraph.putRoleType("in-context");

        graknGraph.putEntityType("animal").plays(parent).plays(father).plays(pChild).plays(fChild);
        graknGraph.putEntityType("context").plays(inContext);

        RelationType parenthood = graknGraph.putRelationType("parenthood").relates(parent).relates(pChild).relates(inContext);
        RelationType fatherhood = graknGraph.putRelationType("fatherhood").superType(parenthood).relates(father).relates(fChild);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(inContext.getLabel(), parenthood.getLabel(), "sub", "sub", fatherhood.getLabel()));

        graknGraph.commit();
    }

    @Test
    public void checkRoleTypeValidSuperOfSelfTypeWhenLinkedToRelationsWhichAreSubsOfEachOther() throws GraknValidationException {
        RoleType insurer = graknGraph.putRoleType("insurer");
        RoleType monoline = graknGraph.putRoleType("monoline").superType(insurer);
        RoleType insured = graknGraph.putRoleType("insured");
        RelationType insure = graknGraph.putRelationType("insure").relates(insurer).relates(insured);
        graknGraph.putRelationType("monoline-insure").relates(monoline).relates(insured).superType(insure);
        graknGraph.commit();
    }

    @Test
    public void whenARoleInARelationIsNotPlayed_TheGraphIsValid() {
        RoleType role1 = graknGraph.putRoleType("role-1");
        RoleType role2 = graknGraph.putRoleType("role-2");
        RelationType relationType = graknGraph.putRelationType("my-relation").relates(role1).relates(role2);

        Instance instance = graknGraph.putEntityType("my-entity").plays(role1).addEntity();

        relationType.addRelation().addRolePlayer(role1, instance);

        graknGraph.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedTwice_TheGraphIsValid() {
        RoleType role1 = graknGraph.putRoleType("role-1");
        RoleType role2 = graknGraph.putRoleType("role-2");
        RelationType relationType = graknGraph.putRelationType("my-relation").relates(role1).relates(role2);

        EntityType entityType = graknGraph.putEntityType("my-entity").plays(role1);
        Instance instance1 = entityType.addEntity();
        Instance instance2 = entityType.addEntity();

        Relation relation = relationType.addRelation();
        relation.addRolePlayer(role1, instance1);
        relation.addRolePlayer(role1, instance2);

        assertThat(relation.rolePlayers(role1), hasItems(instance1, instance2));

        graknGraph.commit();
    }

    @Test
    public void whenARoleInARelationIsPlayedAZillionTimes_TheGraphIsValid() {
        RoleType role1 = graknGraph.putRoleType("role-1");
        RoleType role2 = graknGraph.putRoleType("role-2");
        RelationType relationType = graknGraph.putRelationType("my-relation").relates(role1).relates(role2);

        EntityType entityType = graknGraph.putEntityType("my-entity").plays(role1);

        Relation relation = relationType.addRelation();

        Set<Instance> instances = new HashSet<>();

        int oneZillion = 100;
        for (int i = 0 ; i < oneZillion; i ++) {
            Instance instance = entityType.addEntity();
            instances.add(instance);
            relation.addRolePlayer(role1, instance);
        }

        assertEquals(instances, relation.rolePlayers(role1));

        graknGraph.commit();
    }

    @Test
    public void whenARelationTypeHasOnlyOneRole_TheGraphIsValid() {
        RoleType role = graknGraph.putRoleType("role-1");
        graknGraph.putRelationType("my-relation").relates(role);

        graknGraph.commit();
    }

}