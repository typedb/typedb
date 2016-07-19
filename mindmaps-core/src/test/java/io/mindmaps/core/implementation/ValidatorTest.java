package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class ValidatorTest {
    private final Logger LOG = org.slf4j.LoggerFactory.getLogger(ValidatorTest.class);
    private MindmapsTransactionImpl mindmapsGraph;

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraphAccessManager(){
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
    }
    @After
    public void destroyGraphAccessManager()  throws Exception{
        mindmapsGraph.close();
    }

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
        Instance pacino = mindmapsGraph.putEntity("Pacino", person);
        Instance godfather = mindmapsGraph.putEntity("Godfather", movie);
        EntityType genre = mindmapsGraph.putEntityType("Genre");
        RoleType movieOfGenre = mindmapsGraph.putRoleType("Movie of Genre");
        RoleType movieGenre = mindmapsGraph.putRoleType("Movie Genre");
        Instance crime = mindmapsGraph.putEntity("Crime", genre);
        RelationType movieHasGenre = mindmapsGraph.putRelationType("Movie Has Genre");

        //Construction
        cast.hasRole(feature);
        cast.hasRole(actor);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), cast).
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), movieHasGenre).
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
        Instance kyle = mindmapsGraph.putEntity("kyle", fakeType);
        Instance icke = mindmapsGraph.putEntity("icke", fakeType);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
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
        InstanceImpl kyle = (InstanceImpl) mindmapsGraph.putEntity("kyle", fakeType);
        InstanceImpl icke = (InstanceImpl) mindmapsGraph.putEntity("icke", fakeType);

        Relation relation = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
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
        Instance kyle = mindmapsGraph.putEntity("kyle", fakeType);
        Instance icke = mindmapsGraph.putEntity("icke", fakeType);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
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
        Instance x5 = mindmapsGraph.putEntity("x5", x1);

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
        Instance godfather = mindmapsGraph.putEntity("godfather", movie);

        // add many random actors
        int n = 100;
        for (int i=0; i < n; i++) {
            Instance newPerson = mindmapsGraph.putEntity(String.valueOf(i), person);
            mindmapsGraph.putRelation(UUID.randomUUID().toString(), cast).
                    putRolePlayer(actor, newPerson).putRolePlayer(feature, godfather);
        }

        mindmapsGraph.commit();

        // now try to delete all assertions and then the movie
        godfather = mindmapsGraph.getEntity("godfather");
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
        castingIds.forEach(id -> assertNull(mindmapsGraph.getConcept(id)));

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
        Entity ent1 = mindmapsGraph.putEntity("ent1", ent);
        Entity ent2 = mindmapsGraph.putEntity("ent2", ent);
        mindmapsGraph.addRelation(rel).putRolePlayer(role1, ent1).putRolePlayer(role2, ent2);
        mindmapsGraph.commit();

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION.getMessage(1))
        ));

        mindmapsGraph.putEntity("ent1", ent_t);
        mindmapsGraph.commit();
    }
}