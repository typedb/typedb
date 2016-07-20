package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;

public class OntologyMutationTest {
    private MindmapsTransactionImpl mindmapsGraph;
    private RoleType husband;
    private RoleType wife;
    private RoleType driver;
    private RoleType driven;
    //private RoleType spouse;
    private RelationType marriage;
    //private RelationType union;
    private RelationType carBeingDrivenBy;
    private EntityType person;
    private EntityType woman;
    private EntityType man;
    private EntityType car;
    private Instance alice;
    private Instance bob;
    private Relation relation;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph() throws MindmapsValidationException {
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();

        //spouse = mindmapsGraph.putRoleType("Spouse");
        husband = mindmapsGraph.putRoleType("Husband");//.superType(spouse);
        wife = mindmapsGraph.putRoleType("Wife");
        driver = mindmapsGraph.putRoleType("Driver");
        driven = mindmapsGraph.putRoleType("Driven");

        //union = mindmapsGraph.putRelationType("Union").hasRole(spouse).hasRole(wife);
        marriage = mindmapsGraph.putRelationType("marriage").hasRole(husband).hasRole(wife);
        carBeingDrivenBy = mindmapsGraph.putRelationType("car being driven by").hasRole(driven).hasRole(driver);

        person = mindmapsGraph.putEntityType("Person").playsRole(husband).playsRole(wife);
        man = mindmapsGraph.putEntityType("Man").superType(person);
        woman = mindmapsGraph.putEntityType("Woman").superType(person);
        car = mindmapsGraph.putEntityType("Car");

        alice = mindmapsGraph.putEntity("Alice", woman);
        bob = mindmapsGraph.putEntity("Bob", man);
        relation = mindmapsGraph.addRelation(marriage).putRolePlayer(wife, alice).putRolePlayer(husband, bob);
        mindmapsGraph.commit();
    }
    @After
    public void destroyGraph()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void testChangingInstanceType() throws MindmapsValidationException {
        mindmapsGraph.putEntity("Bob", car);

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(car.getId(), bob.getId(), husband.getId()))
        ));

        mindmapsGraph.commit();
    }

    @Test
    public void testDeletePlaysRole() throws MindmapsValidationException {
        person.deletePlaysRole(wife);

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(woman.getId(), alice.getId(), wife.getId()))
        ));

        mindmapsGraph.commit();
    }

    @Test
    public void testDeleteHasRole() throws MindmapsValidationException {
        marriage.deleteHasRole(husband);

        String roles = "";
        String rolePlayers = "";
        for(Map.Entry<RoleType, Instance> entry: relation.rolePlayers().entrySet()){
            if(entry.getKey() != null)
                roles = roles + entry.getKey().getId() + ",";
            if(entry.getValue() != null)
                rolePlayers = rolePlayers + entry.getValue().getId() + ",";
        }

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_RELATION.getMessage(relation.getId(), marriage.getId(),
                        roles.split(",").length, roles,
                        rolePlayers.split(",").length, roles))
        ));

        mindmapsGraph.commit();
    }

    @Test
    public void testChangeSuperTypeOfEntityType() throws MindmapsValidationException {
        man.superType(car);

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_CASTING.getMessage(man.getId(), bob.getId(), husband.getId()))
        ));

        mindmapsGraph.commit();
    }

    @Test
    public void testChangeIsAbstract() throws MindmapsValidationException{
        man.setAbstract(true);

        expectedException.expect(MindmapsValidationException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(man.getId()))
        ));

        mindmapsGraph.commit();
    }

}
