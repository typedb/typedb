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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.graql.Graql;

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

    public static GraknGraph getGraph() {
        return new CWGraph().graph();
    }

    @Override
    protected void buildOntology() {
        nationalityTarget = graknGraph.putRoleType("has-nationality-owner");
        nationalityValue = graknGraph.putRoleType("has-nationality-value");
        nationalityRelation = graknGraph.putRelationType("has-nationality")
                .hasRole(nationalityTarget).hasRole(nationalityValue);
        nationality = graknGraph.putResourceType("nationality", ResourceType.DataType.STRING)
                .playsRole(nationalityValue);

        propulsionTarget = graknGraph.putRoleType("has-propulsion-owner");
        propulsionValue = graknGraph.putRoleType("has-propulsion-value");
        propulsionRelation = graknGraph.putRelationType("has-propulsion")
                .hasRole(propulsionTarget).hasRole(propulsionValue);
        propulsion = graknGraph.putResourceType("propulsion", ResourceType.DataType.STRING)
                .playsRole(propulsionValue);

        alignmentTarget = graknGraph.putRoleType("has-alignment-owner");
        alignmentValue = graknGraph.putRoleType("has-alignment-value");
        alignmentRelation = graknGraph.putRelationType("has-alignment")
                .hasRole(alignmentTarget).hasRole(alignmentValue);
        alignment = graknGraph.putResourceType("alignment", ResourceType.DataType.STRING)
                .playsRole(alignmentValue);


        enemySource = graknGraph.putRoleType("enemy-source");
        enemyTarget = graknGraph.putRoleType("enemy-target");
        isEnemyOf = graknGraph.putRelationType("is-enemy-of")
                .hasRole(enemySource).hasRole(enemyTarget);

        //owns
        owner = graknGraph.putRoleType("item-owner");
        ownedItem = graknGraph.putRoleType("owned-item");
        owns = graknGraph.putRelationType("owns")
                .hasRole(owner).hasRole(ownedItem);

        //transaction
        seller = graknGraph.putRoleType("seller");
        buyer = graknGraph.putRoleType("buyer");
        transactionItem = graknGraph.putRoleType("transaction-item");
        transaction = graknGraph.putRelationType("transaction")
                .hasRole(seller).hasRole(buyer).hasRole(transactionItem);

        //isPaidBy
        payee = graknGraph.putRoleType("payee");
        payer = graknGraph.putRoleType("payer");
        isPaidBy = graknGraph.putRelationType("is-paid-by")
                .hasRole(payee).hasRole(payer);

        person = graknGraph.putEntityType("person")
                .playsRole(hasKeyTarget)
                .playsRole(seller)
                .playsRole(payee)
                .playsRole(nationalityTarget);

        criminal = graknGraph.putEntityType("criminal")
                .superType(person);

        weapon = graknGraph.putEntityType("weapon")
                .playsRole(hasKeyTarget)
                .playsRole(transactionItem)
                .playsRole(ownedItem);
        rocket = graknGraph.putEntityType("rocket")
                .playsRole(hasKeyTarget)
                .playsRole(transactionItem)
                .playsRole(ownedItem)
                .playsRole(propulsionTarget);
        missile = graknGraph.putEntityType("missile")
                .superType(weapon)
                .playsRole(hasKeyTarget)
                .playsRole(transactionItem);

        country = graknGraph.putEntityType("country")
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
        isEnemyOf.addRelation()
                .putRolePlayer(enemySource, Nono)
                .putRolePlayer(enemyTarget, America);

        //Owns(Nono, Missile)
        owns.addRelation()
                .putRolePlayer(owner, Nono)
                .putRolePlayer(ownedItem, Tomahawk);

        //isPaidBy(West, Nono)
        isPaidBy.addRelation()
                .putRolePlayer(payee, colonelWest)
                .putRolePlayer(payer, Nono);

    }
    @Override
    protected void buildRules() {
        RuleType inferenceRule = graknGraph.getMetaRuleInference();

        //R1: "It is a crime for an American to sell weapons to hostile nations"
        Pattern R1_LHS = Graql.and(
                graknGraph.graql().parsePatterns("$x isa person;$x has nationality 'American';" +
                "$y isa weapon;" +
                "$z isa country;$z has alignment 'hostile';" +
                "(seller: $x, transaction-item: $y, buyer: $z) isa transaction;"));

        Pattern R1_RHS = Graql.and(graknGraph.graql().parsePatterns("$x isa criminal;"));

        inferenceRule.addRule(R1_LHS, R1_RHS);

        //R2: "Missiles are a kind of a weapon"
        Pattern R2_LHS = Graql.and(graknGraph.graql().parsePatterns("$x isa missile;"));
        Pattern R2_RHS = Graql.and(graknGraph.graql().parsePatterns("$x isa weapon;"));

        inferenceRule.addRule(R2_LHS, R2_RHS);

        //R3: "If a country is an enemy of America then it is hostile"
        Pattern R3_LHS = Graql.and(
                graknGraph.graql().parsePatterns("$x isa country;" +
                "($x, $y) isa is-enemy-of;" +
                "$y isa country;$y has name 'America';"));
        Pattern R3_RHS = Graql.and(graknGraph.graql().parsePatterns("$x has alignment 'hostile';"));

        inferenceRule.addRule(R3_LHS, R3_RHS);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        Pattern R4_LHS = Graql.and(graknGraph.graql().parsePatterns("$x isa rocket;$x has propulsion 'gsp';"));
        Pattern R4_RHS = Graql.and(graknGraph.graql().parsePatterns("$x isa missile;"));

        inferenceRule.addRule(R4_LHS, R4_RHS);

        Pattern R5_LHS = Graql.and(
                graknGraph.graql().parsePatterns("$x isa person;" +
                "$y isa country;" +
                "$z isa weapon;" +
                "($x, $y) isa is-paid-by;" +
                "($y, $z) isa owns;"));

        Pattern R5_RHS = Graql.and(graknGraph.graql().parsePatterns("(seller: $x, buyer: $y, transaction-item: $z) isa transaction;"));

        inferenceRule.addRule(R5_LHS, R5_RHS);
    }
}
