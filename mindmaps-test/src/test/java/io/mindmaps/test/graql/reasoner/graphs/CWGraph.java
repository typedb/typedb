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

package io.mindmaps.test.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.RuleType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Pattern;

public class CWGraph extends TestGraph {
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
    
    private static RelationType isEnemyOf, isPaidBy, owns, transaction;
    
    private static RoleType enemySource, enemyTarget;
    private static RoleType owner, ownedItem;
    private static RoleType payee, payer;
    private static RoleType seller, buyer, transactionItem;

    private static Instance colonelWest, Nono, America, Tomahawk;

    public static MindmapsGraph getGraph() {
        return new CWGraph().graph();
    }

    @Override
    protected void buildOntology() {
        nationalityTarget = mindmaps.putRoleType("has-nationality-owner");
        nationalityValue = mindmaps.putRoleType("has-nationality-value");
        nationalityRelation = mindmaps.putRelationType("has-nationality")
                .hasRole(nationalityTarget).hasRole(nationalityValue);
        nationality = mindmaps.putResourceType("nationality", ResourceType.DataType.STRING) 
                .playsRole(nationalityValue);

        propulsionTarget = mindmaps.putRoleType("has-propulsion-owner");
        propulsionValue = mindmaps.putRoleType("has-propulsion-value");
        propulsionRelation = mindmaps.putRelationType("has-propulsion")
                .hasRole(propulsionTarget).hasRole(propulsionValue);
        propulsion = mindmaps.putResourceType("propulsion", ResourceType.DataType.STRING) 
                .playsRole(propulsionValue);

        alignmentTarget = mindmaps.putRoleType("has-alignment-owner");
        alignmentValue = mindmaps.putRoleType("has-alignment-value");
        alignmentRelation = mindmaps.putRelationType("has-alignment")
                .hasRole(alignmentTarget).hasRole(alignmentValue);
        alignment = mindmaps.putResourceType("alignment", ResourceType.DataType.STRING) 
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
                .playsRole(hasKeyTarget)
                .playsRole(seller)
                .playsRole(payee)
                .playsRole(nationalityTarget);

        criminal = mindmaps.putEntityType("criminal")
                .superType(person);

        weapon = mindmaps.putEntityType("weapon")
                .playsRole(hasKeyTarget)
                .playsRole(transactionItem)
                .playsRole(ownedItem);
        rocket = mindmaps.putEntityType("rocket")
                .playsRole(hasKeyTarget)
                .playsRole(transactionItem)
                .playsRole(ownedItem)
                .playsRole(propulsionTarget);
        missile = mindmaps.putEntityType("missile")
                .superType(weapon)
                .playsRole(hasKeyTarget)
                .playsRole(transactionItem);

        country = mindmaps.putEntityType("country")
                .playsRole(hasKeyTarget)
                .playsRole(buyer)
                .playsRole(owner)
                .playsRole(enemyTarget)
                .playsRole(payer)
                .playsRole(enemySource);
    }

    @Override
    protected void buildInstances() {
        colonelWest =  putEntity("colonelWest", person);
        Nono =  putEntity("Nono", country);
        America =  putEntity("America", country);
        Tomahawk =  putEntity("Tomahawk", rocket);

        putResource(colonelWest, nationality, "American", nationalityRelation, nationalityTarget, nationalityValue);
        putResource(Tomahawk, propulsion, "gsp", propulsionRelation, propulsionTarget, propulsionValue);
    }

    @Override
    protected void buildRelations() {
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
    @Override
    protected void buildRules() {
        RuleType inferenceRule = mindmaps.getMetaRuleInference();

        //R1: "It is a crime for an American to sell weapons to hostile nations"
        Pattern R1_LHS = Graql.and(
                mindmaps.graql().parsePatterns("$x isa person;$x has nationality 'American';" +
                "$y isa weapon;" +
                "$z isa country;$z has alignment 'hostile';" +
                "(seller: $x, transaction-item: $y, buyer: $z) isa transaction;"));

        Pattern R1_RHS = Graql.and(mindmaps.graql().parsePatterns("$x isa criminal;"));

        mindmaps.addRule(R1_LHS, R1_RHS, inferenceRule);

        //R2: "Missiles are a kind of a weapon"
        Pattern R2_LHS = Graql.and(mindmaps.graql().parsePatterns("$x isa missile;"));
        Pattern R2_RHS = Graql.and(mindmaps.graql().parsePatterns("$x isa weapon;"));

        mindmaps.addRule(R2_LHS, R2_RHS, inferenceRule);

        //R3: "If a country is an enemy of America then it is hostile"
        Pattern R3_LHS = Graql.and(
                mindmaps.graql().parsePatterns("$x isa country;" +
                "($x, $y) isa is-enemy-of;" +
                "$y isa country;$y has name 'America';"));
        Pattern R3_RHS = Graql.and(mindmaps.graql().parsePatterns("$x has alignment 'hostile';"));

        mindmaps.addRule(R3_LHS, R3_RHS, inferenceRule);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        Pattern R4_LHS = Graql.and(mindmaps.graql().parsePatterns("$x isa rocket;$x has propulsion 'gsp';"));
        Pattern R4_RHS = Graql.and(mindmaps.graql().parsePatterns("$x isa missile;"));

        mindmaps.addRule(R4_LHS, R4_RHS, inferenceRule);

        Pattern R5_LHS = Graql.and(
                mindmaps.graql().parsePatterns("$x isa person;" +
                "$y isa country;" +
                "$z isa weapon;" +
                "($x, $y) isa is-paid-by;" +
                "($y, $z) isa owns;"));

        Pattern R5_RHS = Graql.and(mindmaps.graql().parsePatterns("(seller: $x, buyer: $y, transaction-item: $z) isa transaction;"));

        mindmaps.addRule(R5_LHS, R5_RHS, inferenceRule);
    }
}
