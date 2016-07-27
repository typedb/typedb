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

package io.mindmaps.reasoner.graphs;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;

import java.util.UUID;

public class CWGraph {

    private static MindmapsTransaction mindmaps;

    private static EntityType person, criminal, weapon, rocket, missile, country;
    private static ResourceType<String> nationality;
    private static ResourceType<String> propulsion;
    private static ResourceType<String> alignment;
    private static RelationType hasResource, isEnemyOf, isPaidBy, owns, transaction;

    private static RoleType hasResourceTarget, hasResourceValue;
    private static RoleType enemySource, enemyTarget;
    private static RoleType owner, ownedItem;
    private static RoleType payee, payer;
    private static RoleType seller, buyer, transactionItem;

    private static Instance colonelWest, Nono, America, Tomahawk;

    private CWGraph() {
    }

    public static MindmapsTransaction getTransaction() {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmaps = graph.newTransaction();
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

        nationality = mindmaps.putResourceType("nationality", Data.STRING).playsRole(hasResourceValue);
        propulsion = mindmaps.putResourceType("propulsion", Data.STRING).playsRole(hasResourceValue);
        alignment = mindmaps.putResourceType("alignment", Data.STRING).playsRole(hasResourceValue);

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


        person = mindmaps.putEntityType("person").setValue("person")
                .playsRole(seller).playsRole(payee).playsRole(hasResourceTarget);
        criminal = mindmaps.putEntityType("criminal").setValue("criminal").superType(person);

        //device = mindmaps.putEntityType("device").setValue("device");
        weapon = mindmaps.putEntityType("weapon").setValue("weapon")
                .playsRole(transactionItem).playsRole(ownedItem).playsRole(hasResourceTarget);//.superEntity(device);
        rocket = mindmaps.putEntityType("rocket").setValue("rocket")
                .playsRole(hasResourceTarget).playsRole(transactionItem).playsRole(ownedItem);
        missile = mindmaps.putEntityType("missile").setValue("missile").superType(weapon)
                .playsRole(transactionItem).playsRole(hasResourceTarget);


        country = mindmaps.putEntityType("country").setValue("country")
                .playsRole(buyer).playsRole(owner).playsRole(enemyTarget).playsRole(payer).playsRole(enemySource).playsRole(hasResourceTarget);


    }

    private static void buildInstances() {

        colonelWest = putEntity(person, "colonelWest");
        Nono = putEntity(country, "Nono");
        America = putEntity(country, "America");
        Tomahawk = putEntity(rocket, "Tomahawk");

        putResource(colonelWest, nationality, "American");
        putResource(Tomahawk, propulsion, "gsp");

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

        Rule r1 = mindmaps.putRule("R1", inferenceRule);
        Rule r2 = mindmaps.putRule("R2", inferenceRule);
        Rule r3 = mindmaps.putRule("R3", inferenceRule);
        Rule r4 = mindmaps.putRule("R4", inferenceRule);
        Rule r5 = mindmaps.putRule("R5", inferenceRule);

        //R1: "It is a crime for an American to sell weapons to hostile nations"
        String R1_LHS = "match " +
                "$x isa person;\n" +
                "$x has nationality 'American';\n" +
                "$y isa weapon;\n" +
                "$z isa country;\n" +
                "$z has alignment 'hostile';\n" +
                "($x, $y, $z) isa transaction;" +
                "select $x";

        String R1_RHS = "match $x isa criminal";

        r1.setLHS(R1_LHS);
        r1.setRHS(R1_RHS);

        //R2: "Missiles are a kind of a weapon"
        String  R2_LHS = "match $x isa missile;";
        String R2_RHS = "match $x isa weapon";

        r2.setLHS(R2_LHS);
        r2.setRHS(R2_RHS);

        //R3: "If a country is an enemy of America then it is hostile"
        String R3_LHS = "match " +
                "$x isa country;\n" +
                "($x, $y) isa is-enemy-of;\n" +
                "$y isa country;\n" +
                "$y value 'America';" +
                "select $x";
        String R3_RHS = "match $x has alignment 'hostile'";

        r3.setLHS(R3_LHS);
        r3.setRHS(R3_RHS);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        String R4_LHS = "match $x isa rocket;$x has propulsion 'gsp'";
        String R4_RHS = "match $x isa missile";

        r4.setLHS(R4_LHS);
        r4.setRHS(R4_RHS);

        String R5_LHS = "match "+
                "$x isa person;\n" +
                "$y isa country;\n" +
                "$z isa weapon;\n" +
                "($x, $y) isa is-paid-by;\n" +
                "($y, $z) isa owns";

        String R5_RHS = "match ($x, $y, $z) isa transaction";

        r5.setLHS(R5_LHS);
        r5.setRHS(R5_RHS);

    }

    private static Instance putEntity(EntityType type, String name) {
        return mindmaps.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type).setValue(name);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = mindmaps.putResource(UUID.randomUUID().toString(), resourceType).setValue(resource);

        mindmaps.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, instance)
                .putRolePlayer(hasResourceValue, resourceInstance);
    }


}
