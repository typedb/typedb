/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graph.internal;

import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.exception.InvalidConceptTypeException;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.util.ErrorMessage;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        RelationType cast = mindmapsGraph.putRelationType("Cast");
        RoleType feature = mindmapsGraph.putRoleType("Feature");
        RoleType actor = mindmapsGraph.putRoleType("Actor");
        EntityType movie = mindmapsGraph.putEntityType("Movie");
        EntityType person = mindmapsGraph.putEntityType("Person");
        Instance pacino = mindmapsGraph.addEntity(person);
        Instance godfather = mindmapsGraph.addEntity(movie);
        EntityType genre = mindmapsGraph.putEntityType("Genre");
        RoleType movieOfGenre = mindmapsGraph.putRoleType("Movie of Genre");
        RoleType movieGenre = mindmapsGraph.putRoleType("Movie Genre");
        Instance crime = mindmapsGraph.addEntity(genre);
        RelationType movieHasGenre = mindmapsGraph.putRelationType("Movie Has Genre");

        //Construction
        cast.hasRole(feature);
        cast.hasRole(actor);

        mindmapsGraph.addRelation(cast).
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);

        mindmapsGraph.addRelation(movieHasGenre).
                putRolePlayer(movieOfGenre, godfather).putRolePlayer(movieGenre, crime);

        movieHasGenre.hasRole(movieOfGenre);
        movieHasGenre.hasRole(movieGenre);

        movie.playsRole(movieOfGenre);
        person.playsRole(actor);
        movie.playsRole(feature);
        genre.playsRole(movieGenre);

        boolean exceptionThrown = false;
        try {
            mindmapsGraph.validateGraph();
        } catch (MindmapsValidationException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);
    }

    @Test
    public void castingValidationOfRoleTypeAndPlaysRoleEdge(){
        EntityType fakeType = mindmapsGraph.putEntityType("Fake Concept");
        RelationType relationType = mindmapsGraph.putRelationType("kicks");
        RoleType kicker = mindmapsGraph.putRoleType("kicker");
        RoleType kickee = mindmapsGraph.putRoleType("kickee");
        Instance kyle = mindmapsGraph.addEntity(fakeType);
        Instance icke = mindmapsGraph.addEntity(fakeType);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.addRelation(relationType).
                putRolePlayer(kicker, kyle).putRolePlayer(kickee, icke);

        boolean failure = false;
        try {
            mindmapsGraph.validateGraph();
        } catch (MindmapsValidationException e) {
            failure = true;
        }
        assertTrue(failure);

        Validator validator = new Validator(mindmapsGraph);
        assertFalse(validator.validate());
        assertEquals(6, validator.getErrorsFound().size());

        CastingImpl casting1 = (CastingImpl) assertion.getMappingCasting().toArray()[0];
        CastingImpl casting2 = (CastingImpl) assertion.getMappingCasting().toArray()[1];
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_CASTING.getMessage(
                casting1.getRolePlayer().type().getId(), casting1.getRolePlayer().getId(), casting1.getRole().getId())));
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_CASTING.getMessage(
                casting2.getRolePlayer().type().getId(), casting2.getRolePlayer().getId(), casting2.getRole().getId())));
    }

    @Test
    public void hasRoleEdgeTestFail(){
        RoleType alone = mindmapsGraph.putRoleType("alone");
        Validator validator = new Validator(mindmapsGraph);
        assertFalse(validator.validate());
        assertEquals(1, validator.getErrorsFound().size());
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_ROLE_TYPE.getMessage(alone.getId())));
    }

    @Test
    public void relationTypeHasRolesTest(){
        RelationType alone = mindmapsGraph.putRelationType("alone");
        Validator validator = new Validator(mindmapsGraph);
        assertFalse(validator.validate());
        assertEquals(1, validator.getErrorsFound().size());
        assertTrue(expectedErrorFound(validator, ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(alone.getId())));
    }

    @Test
    public void validateAssertionFail(){
        EntityType fakeType = mindmapsGraph.putEntityType("Fake Concept");
        RelationType relationType = mindmapsGraph.putRelationType("kicks");
        RoleType kicker = mindmapsGraph.putRoleType("kicker");
        RoleType kickee = mindmapsGraph.putRoleType("kickee");
        InstanceImpl kyle = (InstanceImpl) mindmapsGraph.addEntity(fakeType);
        InstanceImpl icke = (InstanceImpl) mindmapsGraph.addEntity(fakeType);

        mindmapsGraph.addRelation(relationType).
                putRolePlayer(kicker, kyle).putRolePlayer(kickee, icke);

        Validator validator = new Validator(mindmapsGraph);
        assertFalse(validator.validate());

        assertEquals(6, validator.getErrorsFound().size());
        assertTrue(expectedErrorFound(validator, "invalid structure."));
    }

    @Test
    public void validateCastingFail(){
        EntityType fakeType = mindmapsGraph.putEntityType("Fake Concept");
        RelationType relationType = mindmapsGraph.putRelationType("kicks");
        RoleType kicker = mindmapsGraph.putRoleType("kicker");
        RoleType kickee = mindmapsGraph.putRoleType("kickee");
        Instance kyle = mindmapsGraph.addEntity(fakeType);
        Instance icke = mindmapsGraph.addEntity(fakeType);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.addRelation(relationType).
                putRolePlayer(kicker, kyle).putRolePlayer(kickee, icke);
        CastingImpl casting = (CastingImpl) assertion.getMappingCasting().toArray()[0];
        Validator validator = new Validator(mindmapsGraph);
        assertFalse(validator.validate());
        assertEquals(6, validator.getErrorsFound().size());
    }

    @Test
    public void validateIsAbstract(){
        EntityType x1 = mindmapsGraph.putEntityType("x1");
        EntityType x2 = mindmapsGraph.putEntityType("x2");
        EntityType x3 = mindmapsGraph.putEntityType("x3");
        EntityType x4 = mindmapsGraph.putEntityType("x4");
        Instance x5 = mindmapsGraph.addEntity(x1);

        x1.setAbstract(true);
        x4.setAbstract(true);

        x4.superType(x3);

        Validator validator = new Validator(mindmapsGraph);

        validator.validate();

        assertTrue((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x1.getId()))));
        assertFalse((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x2.getId()))));
        assertFalse((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x3.getId()))));
        assertFalse((expectedErrorFound(validator, ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(x4.getId()))));
    }

    @Test
    public void testValidateAfterManualAssertionDelete() throws MindmapsValidationException {
        mindmapsGraph.initialiseMetaConcepts();

        // ontology
        EntityType person = mindmapsGraph.putEntityType("person");
        EntityType movie = mindmapsGraph.putEntityType("movie");
        RelationType cast = mindmapsGraph.putRelationType("cast");
        RoleType feature = mindmapsGraph.putRoleType("feature");
        RoleType actor = mindmapsGraph.putRoleType("actor");
        cast.hasRole(feature).hasRole(actor);
        person.playsRole(actor);
        movie.playsRole(feature);

        // add a single movie
        Instance godfather = mindmapsGraph.addEntity(movie);

        // add many random actors
        int n = 100;
        for (int i=0; i < n; i++) {
            Instance newPerson = mindmapsGraph.addEntity(person);
            mindmapsGraph.addRelation(cast).
                    putRolePlayer(actor, newPerson).putRolePlayer(feature, godfather);
        }

        mindmapsGraph.commit();

        // now try to delete all assertions and then the movie
        godfather = mindmapsGraph.getEntityType("movie").instances().iterator().next();
        Collection<Relation> assertions = godfather.relations();
        Set<String> assertionIds = new HashSet<>();
        Set<String> castingIds = new HashSet<>();
        for (Relation a : assertions) {
            assertionIds.add(a.getId());
            ((RelationImpl) a).getMappingCasting().forEach(c -> castingIds.add(c.getId()));
            a.delete();
        }
        godfather.delete();

        mindmapsGraph.commit();

        assertionIds.forEach(id -> assertNull(mindmapsGraph.getConcept(id)));

        // assert the movie is gone
        assertNull(mindmapsGraph.getEntity("godfather"));

    }

    @Test
    public void testChangeTypeOfEntity() throws MindmapsValidationException {
        RoleType role1 = mindmapsGraph.putRoleType("role1");
        RoleType role2 = mindmapsGraph.putRoleType("role2");
        RelationType rel = mindmapsGraph.putRelationType("rel").hasRole(role1).hasRole(role2);
        EntityType ent = mindmapsGraph.putEntityType("ent").playsRole(role1).playsRole(role2);
        EntityType ent_t = mindmapsGraph.putEntityType("ent_t");
        Entity ent1 = mindmapsGraph.addEntity(ent);
        Entity ent2 = mindmapsGraph.addEntity(ent);
        mindmapsGraph.addRelation(rel).putRolePlayer(role1, ent1).putRolePlayer(role2, ent2);
        mindmapsGraph.commit();

        expectedException.expect(InvalidConceptTypeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.IMMUTABLE_TYPE.getMessage(ent1, ent_t, ent))
        ));

        mindmapsGraph.putEntity(ent1.getId(), ent_t);
    }

    @Test
    public void testRoleTypeCanPlayRoleIfAbstract() throws MindmapsValidationException {
        RoleType role1 = mindmapsGraph.putRoleType("role1").setAbstract(true);
        RoleType role2 = mindmapsGraph.putRoleType("role2").setAbstract(true);
        EntityType entityType = mindmapsGraph.putEntityType("my type").playsRole(role1).playsRole(role2);
        mindmapsGraph.commit();
    }

    @Test
    public void testNormalRelationshipWithTwoPlaysRole() throws MindmapsValidationException {
        RoleType characterBeingPlayed = mindmapsGraph.putRoleType("Character being played");
        RoleType personPlayingCharacter = mindmapsGraph.putRoleType("Person Playing Char");
        RelationType playsChar = mindmapsGraph.putRelationType("Plays Char").hasRole(characterBeingPlayed).hasRole(personPlayingCharacter);

        EntityType person = mindmapsGraph.putEntityType("person").playsRole(characterBeingPlayed).playsRole(personPlayingCharacter);
        EntityType character = mindmapsGraph.putEntityType("character").playsRole(characterBeingPlayed);

        Entity matt = mindmapsGraph.addEntity(person);
        Entity walker = mindmapsGraph.addEntity(character);

        mindmapsGraph.addRelation(playsChar).
                putRolePlayer(personPlayingCharacter, matt).
                putRolePlayer(characterBeingPlayed, walker);

        mindmapsGraph.commit();
    }

}