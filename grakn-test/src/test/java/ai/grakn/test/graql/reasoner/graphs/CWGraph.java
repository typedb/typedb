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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;

import java.util.UUID;

public class CWGraph {

    private static GraknGraph grakn;

    private static EntityType person, criminal, weapon, rocket, missile, country;
    

    private static ResourceType<String> alignment;
    private static RelationType alignmentRelation;
    private static RoleType alignmentValue, alignmentTarget;

    private static ResourceType<String> propulsion;
    private static RelationType propulsionRelation;
    private static RoleType propulsionValue, propulsionTarget;

    private static ResourceType<String> nationality;
    private static RelationType nationalityRelation;
    private static RoleType nationalityValue, nationalityTarget;
    
    private static RelationType hasResource, isEnemyOf, isPaidBy, owns, transaction;

    private static RoleType hasResourceTarget, hasResourceValue;
    private static RoleType enemySource, enemyTarget;
    private static RoleType owner, ownedItem;
    private static RoleType payee, payer;
    private static RoleType seller, buyer, transactionItem;

    private static Instance colonelWest, Nono, America, Tomahawk;

    public static GraknGraph getGraph() {
        grakn = Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        buildGraph();

        try {
            grakn.commit();
        } catch (GraknValidationException e) {
            System.out.println(e.getMessage());
        }

        return grakn;
    }

    private static void buildGraph() {
        buildOntology();
        buildInstances();
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {
        hasResourceTarget = grakn.putRoleType("has-resource-target");
        hasResourceValue = grakn.putRoleType("has-resource-value");
        hasResource = grakn.putRelationType("has-resource")
                .hasRole(hasResourceTarget).hasRole(hasResourceValue);

        nationalityTarget = grakn.putRoleType("has-nationality-owner");
        nationalityValue = grakn.putRoleType("has-nationality-value");
        nationalityRelation = grakn.putRelationType("has-nationality")
                .hasRole(nationalityTarget).hasRole(nationalityValue);
        nationality = grakn.putResourceType("nationality", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(nationalityValue);

        propulsionTarget = grakn.putRoleType("has-propulsion-owner");
        propulsionValue = grakn.putRoleType("has-propulsion-value");
        propulsionRelation = grakn.putRelationType("has-propulsion")
                .hasRole(propulsionTarget).hasRole(propulsionValue);
        propulsion = grakn.putResourceType("propulsion", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(propulsionValue);

        alignmentTarget = grakn.putRoleType("has-alignment-owner");
        alignmentValue = grakn.putRoleType("has-alignment-value");
        alignmentRelation = grakn.putRelationType("has-alignment")
                .hasRole(alignmentTarget).hasRole(alignmentValue);
        alignment = grakn.putResourceType("alignment", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(alignmentValue);


        enemySource = grakn.putRoleType("enemy-source");
        enemyTarget = grakn.putRoleType("enemy-target");
        isEnemyOf = grakn.putRelationType("is-enemy-of")
                .hasRole(enemySource).hasRole(enemyTarget);

        //owns
        owner = grakn.putRoleType("item-owner");
        ownedItem = grakn.putRoleType("owned-item");
        owns = grakn.putRelationType("owns")
                .hasRole(owner).hasRole(ownedItem);


        //transaction
        seller = grakn.putRoleType("seller");
        buyer = grakn.putRoleType("buyer");
        transactionItem = grakn.putRoleType("transaction-item");
        transaction = grakn.putRelationType("transaction")
                .hasRole(seller).hasRole(buyer).hasRole(transactionItem);

        //isPaidBy
        payee = grakn.putRoleType("payee");
        payer = grakn.putRoleType("payer");
        isPaidBy = grakn.putRelationType("is-paid-by")
                .hasRole(payee).hasRole(payer);


        person = grakn.putEntityType("person")
                .playsRole(seller)
                .playsRole(payee)
                .playsRole(hasResourceTarget)
                .playsRole(nationalityTarget);

        criminal = grakn.putEntityType("criminal").superType(person);

        //device = grakn.putEntityType("device").setValue("device");
        weapon = grakn.putEntityType("weapon")
                .playsRole(transactionItem).playsRole(ownedItem).playsRole(hasResourceTarget);//.superEntity(device);
        rocket = grakn.putEntityType("rocket")
                .playsRole(hasResourceTarget)
                .playsRole(transactionItem)
                .playsRole(ownedItem)
                .playsRole(propulsionTarget);
        missile = grakn.putEntityType("missile").superType(weapon)
                .playsRole(transactionItem).playsRole(hasResourceTarget);


        country = grakn.putEntityType("country")
                .playsRole(buyer).playsRole(owner).playsRole(enemyTarget).playsRole(payer).playsRole(enemySource).playsRole(hasResourceTarget);

    }

    private static void buildInstances() {
        colonelWest = grakn.putEntity("colonelWest", person);
        Nono = grakn.putEntity("Nono", country);
        America = grakn.putEntity("America", country);
        Tomahawk = grakn.putEntity("Tomahawk", rocket);

        putResource(colonelWest, nationality, "American", nationalityRelation, nationalityTarget, nationalityValue);
        putResource(Tomahawk, propulsion, "gsp", propulsionRelation, propulsionTarget, propulsionValue);
    }

    private static void buildRelations() {
        //Enemy(Nono, America)
        grakn.addRelation(isEnemyOf)
                .putRolePlayer(enemySource, Nono)
                .putRolePlayer(enemyTarget, America);

        //Owns(Nono, Missile)
        grakn.addRelation(owns)
                .putRolePlayer(owner, Nono)
                .putRolePlayer(ownedItem, Tomahawk);

        //isPaidBy(West, Nono)
        grakn.addRelation(isPaidBy)
                .putRolePlayer(payee, colonelWest)
                .putRolePlayer(payer, Nono);

    }
    private static void buildRules() {
        RuleType inferenceRule = grakn.getMetaRuleInference();

        //R1: "It is a crime for an American to sell weapons to hostile nations"
        String R1_LHS =
                "$x isa person;$x has nationality 'American';" +
                "$y isa weapon;" +
                "$z isa country;$z has alignment 'hostile';" +
                "(seller: $x, transaction-item: $y, buyer: $z) isa transaction;";

        String R1_RHS = "$x isa criminal;";

        grakn.putRule("R1", R1_LHS, R1_RHS, inferenceRule);

        //R2: "Missiles are a kind of a weapon"
        String  R2_LHS = "$x isa missile;";
        String R2_RHS = "$x isa weapon;";

        grakn.putRule("R2", R2_LHS, R2_RHS, inferenceRule);

        //R3: "If a country is an enemy of America then it is hostile"
        String R3_LHS =
                "$x isa country;" +
                "($x, $y) isa is-enemy-of;" +
                "$y isa country;$y id 'America';";
        String R3_RHS = "$x has alignment 'hostile';";

        grakn.putRule("R3", R3_LHS, R3_RHS, inferenceRule);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        String R4_LHS = "$x isa rocket;$x has propulsion 'gsp';";
        String R4_RHS = "$x isa missile;";

        grakn.putRule("R4", R4_LHS, R4_RHS, inferenceRule);

        String R5_LHS =
                "$x isa person;" +
                "$y isa country;" +
                "$z isa weapon;" +
                "($x, $y) isa is-paid-by;" +
                "($y, $z) isa owns;";

        String R5_RHS = "(seller: $x, buyer: $y, transaction-item: $z) isa transaction;";

        grakn.putRule("R5", R5_LHS, R5_RHS, inferenceRule);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = grakn.putResource(resource, resourceType);

        grakn.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, instance)
                .putRolePlayer(hasResourceValue, resourceInstance);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource, RelationType relationType,
                                        RoleType targetRole, RoleType valueRole) {
        Resource resourceInstance = grakn.putResource(resource, resourceType);

        grakn.addRelation(relationType)
                .putRolePlayer(targetRole, instance)
                .putRolePlayer(valueRole, resourceInstance);
    }
}
