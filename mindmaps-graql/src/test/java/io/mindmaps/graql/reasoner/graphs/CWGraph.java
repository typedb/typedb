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

package io.mindmaps.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.RuleType;
import io.mindmaps.factory.MindmapsTestGraphFactory;

public class CWGraph {

    private static MindmapsGraph mindmaps;

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

    public static MindmapsGraph getGraph() {
        mindmaps = MindmapsTestGraphFactory.newEmptyGraph();
        buildGraph();

        try {
            mindmaps.commit();
        } catch (MindmapsValidationException e) {
            System.out.println(e.getMessage());
        }

        return mindmaps;
    }

    private static void buildGraph() {
        buildOntology();
        buildInstances();
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {
        hasResourceTarget = mindmaps.putRoleType("has-resource-target");
        hasResourceValue = mindmaps.putRoleType("has-resource-value");
        hasResource = mindmaps.putRelationType("has-resource")
                .hasRole(hasResourceTarget).hasRole(hasResourceValue);

        nationalityTarget = mindmaps.putRoleType("has-nationality-owner");
        nationalityValue = mindmaps.putRoleType("has-nationality-value");
        nationalityRelation = mindmaps.putRelationType("has-nationality")
                .hasRole(nationalityTarget).hasRole(nationalityValue);
        nationality = mindmaps.putResourceType("nationality", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(nationalityValue);

        propulsionTarget = mindmaps.putRoleType("has-propulsion-owner");
        propulsionValue = mindmaps.putRoleType("has-propulsion-value");
        propulsionRelation = mindmaps.putRelationType("has-propulsion")
                .hasRole(propulsionTarget).hasRole(propulsionValue);
        propulsion = mindmaps.putResourceType("propulsion", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(propulsionValue);

        alignmentTarget = mindmaps.putRoleType("has-alignment-owner");
        alignmentValue = mindmaps.putRoleType("has-alignment-value");
        alignmentRelation = mindmaps.putRelationType("has-alignment")
                .hasRole(alignmentTarget).hasRole(alignmentValue);
        alignment = mindmaps.putResourceType("alignment", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(alignmentValue);


        enemySource = mindmaps.putRoleType("enemy-source");
        enemyTarget = mindmaps.putRoleType("enemy-target");
        isEnemyOf = mindmaps.putRelationType("is-enemy-of")
                .hasRole(enemySource).hasRole(enemyTarget);

        //owns
        owner = mindmaps.putRoleType("item-owner");
        ownedItem = mindmaps.putRoleType("owned-item");
        owns = mindmaps.putRelationType("owns")
                .hasRole(owner).hasRole(ownedItem);


        //transaction
        seller = mindmaps.putRoleType("seller");
        buyer = mindmaps.putRoleType("buyer");
        transactionItem = mindmaps.putRoleType("transaction-item");
        transaction = mindmaps.putRelationType("transaction")
                .hasRole(seller).hasRole(buyer).hasRole(transactionItem);

        //isPaidBy
        payee = mindmaps.putRoleType("payee");
        payer = mindmaps.putRoleType("payer");
        isPaidBy = mindmaps.putRelationType("is-paid-by")
                .hasRole(payee).hasRole(payer);


        person = mindmaps.putEntityType("person")
                .playsRole(seller)
                .playsRole(payee)
                .playsRole(hasResourceTarget)
                .playsRole(nationalityTarget);

        criminal = mindmaps.putEntityType("criminal").superType(person);

        //device = mindmaps.putEntityType("device").setValue("device");
        weapon = mindmaps.putEntityType("weapon")
                .playsRole(transactionItem).playsRole(ownedItem).playsRole(hasResourceTarget);//.superEntity(device);
        rocket = mindmaps.putEntityType("rocket")
                .playsRole(hasResourceTarget)
                .playsRole(transactionItem)
                .playsRole(ownedItem)
                .playsRole(propulsionTarget);
        missile = mindmaps.putEntityType("missile").superType(weapon)
                .playsRole(transactionItem).playsRole(hasResourceTarget);


        country = mindmaps.putEntityType("country")
                .playsRole(buyer).playsRole(owner).playsRole(enemyTarget).playsRole(payer).playsRole(enemySource).playsRole(hasResourceTarget);

    }

    private static void buildInstances() {
        colonelWest = mindmaps.putEntity("colonelWest", person);
        Nono = mindmaps.putEntity("Nono", country);
        America = mindmaps.putEntity("America", country);
        Tomahawk = mindmaps.putEntity("Tomahawk", rocket);

        putResource(colonelWest, nationality, "American", nationalityRelation, nationalityTarget, nationalityValue);
        putResource(Tomahawk, propulsion, "gsp", propulsionRelation, propulsionTarget, propulsionValue);
    }

    private static void buildRelations() {
        //Enemy(Nono, America)
        mindmaps.addRelation(isEnemyOf)
                .putRolePlayer(enemySource, Nono)
                .putRolePlayer(enemyTarget, America);

        //Owns(Nono, Missile)
        mindmaps.addRelation(owns)
                .putRolePlayer(owner, Nono)
                .putRolePlayer(ownedItem, Tomahawk);

        //isPaidBy(West, Nono)
        mindmaps.addRelation(isPaidBy)
                .putRolePlayer(payee, colonelWest)
                .putRolePlayer(payer, Nono);

    }
    private static void buildRules() {
        RuleType inferenceRule = mindmaps.getMetaRuleInference();

        //R1: "It is a crime for an American to sell weapons to hostile nations"
        String R1_LHS = "match " +
                "$x isa person;$x has nationality 'American';" +
                "$y isa weapon;" +
                "$z isa country;$z has alignment 'hostile';" +
                "(seller $x, transaction-item $y, buyer $z) isa transaction;" +
                "select $x";

        String R1_RHS = "match $x isa criminal";

        mindmaps.putRule("R1", R1_LHS, R1_RHS, inferenceRule);

        //R2: "Missiles are a kind of a weapon"
        String  R2_LHS = "match $x isa missile;";
        String R2_RHS = "match $x isa weapon";

        mindmaps.putRule("R2", R2_LHS, R2_RHS, inferenceRule);

        //R3: "If a country is an enemy of America then it is hostile"
        String R3_LHS = "match " +
                "$x isa country;" +
                "($x, $y) isa is-enemy-of;" +
                "$y isa country;$y id 'America';" +
                "select $x";
        String R3_RHS = "match $x has alignment 'hostile'";

        mindmaps.putRule("R3", R3_LHS, R3_RHS, inferenceRule);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        String R4_LHS = "match $x isa rocket;$x has propulsion 'gsp'";
        String R4_RHS = "match $x isa missile";

        mindmaps.putRule("R4", R4_LHS, R4_RHS, inferenceRule);

        String R5_LHS = "match "+
                "$x isa person;" +
                "$y isa country;" +
                "$z isa weapon;" +
                "($x, $y) isa is-paid-by;\n" +
                "($y, $z) isa owns";

        String R5_RHS = "match (seller $x, buyer $y, transaction-item $z) isa transaction";

        mindmaps.putRule("R5", R5_LHS, R5_RHS, inferenceRule);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = mindmaps.putResource(resource, resourceType);

        mindmaps.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, instance)
                .putRolePlayer(hasResourceValue, resourceInstance);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource, RelationType relationType,
                                        RoleType targetRole, RoleType valueRole) {
        Resource resourceInstance = mindmaps.putResource(resource, resourceType);

        mindmaps.addRelation(relationType)
                .putRolePlayer(targetRole, instance)
                .putRolePlayer(valueRole, resourceInstance);
    }


}
