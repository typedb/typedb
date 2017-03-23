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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;

import java.util.function.Consumer;

public class CWGraph extends TestGraph {

    private static ResourceType<String> key;

    private static EntityType person, criminal, weapon, rocket, missile, country;
    
    private static ResourceType<String> alignment;
    private static RoleType alignmentValue, alignmentTarget;

    private static ResourceType<String> propulsion;
    private static RoleType propulsionValue, propulsionTarget;

    private static ResourceType<String> nationality;
    private static RoleType nationalityValue, nationalityTarget;
    
    private static RelationType isEnemyOf, isPaidBy, owns, transaction;
    
    private static RoleType enemySource, enemyTarget;
    private static RoleType owner, ownedItem;
    private static RoleType payee, payer;
    private static RoleType seller, buyer, transactionItem;

    private static Instance colonelWest, Nono, America, Tomahawk;

    public static Consumer<GraknGraph> get() {
        return new CWGraph().build();
    }

    @Override
    protected void buildOntology(GraknGraph graph) {
        key = graph.putResourceType("name", ResourceType.DataType.STRING);

        nationalityTarget = graph.putRoleType("has-nationality-owner");
        nationalityValue = graph.putRoleType("has-nationality-value");
        nationality = graph.putResourceType("nationality", ResourceType.DataType.STRING)
                .playsRole(nationalityValue);

        propulsionTarget = graph.putRoleType("has-propulsion-owner");
        propulsionValue = graph.putRoleType("has-propulsion-value");
        propulsion = graph.putResourceType("propulsion", ResourceType.DataType.STRING)
                .playsRole(propulsionValue);

        alignmentTarget = graph.putRoleType("has-alignment-owner");
        alignmentValue = graph.putRoleType("has-alignment-value");
        alignment = graph.putResourceType("alignment", ResourceType.DataType.STRING)
                .playsRole(alignmentValue);

        enemySource = graph.putRoleType("enemy-source");
        enemyTarget = graph.putRoleType("enemy-target");
        isEnemyOf = graph.putRelationType("is-enemy-of")
                .hasRole(enemySource).hasRole(enemyTarget);

        //owns
        owner = graph.putRoleType("item-owner");
        ownedItem = graph.putRoleType("owned-item");
        owns = graph.putRelationType("owns")
                .hasRole(owner).hasRole(ownedItem);

        //transaction
        seller = graph.putRoleType("seller");
        buyer = graph.putRoleType("buyer");
        transactionItem = graph.putRoleType("transaction-item");
        transaction = graph.putRelationType("transaction")
                .hasRole(seller).hasRole(buyer).hasRole(transactionItem);

        //isPaidBy
        payee = graph.putRoleType("payee");
        payer = graph.putRoleType("payer");
        isPaidBy = graph.putRelationType("is-paid-by")
                .hasRole(payee).hasRole(payer);

        person = graph.putEntityType("person")
                .playsRole(seller)
                .playsRole(payee);
        person.hasResource(key);
        person.hasResource(nationality);

        criminal = graph.putEntityType("criminal")
                .superType(person);

        weapon = graph.putEntityType("weapon")
                .playsRole(transactionItem)
                .playsRole(ownedItem);
        weapon.hasResource(key);

        rocket = graph.putEntityType("rocket")
                .playsRole(transactionItem)
                .playsRole(ownedItem);
        rocket.hasResource(key);
        rocket.hasResource(propulsion);

        missile = graph.putEntityType("missile")
                .superType(weapon)
                .playsRole(transactionItem);
        missile.hasResource(key);

        country = graph.putEntityType("country")
                .playsRole(buyer)
                .playsRole(owner)
                .playsRole(enemyTarget)
                .playsRole(payer)
                .playsRole(enemySource);
        country.hasResource(key);
        country.hasResource(alignment);
    }

    @Override
    protected void buildInstances(GraknGraph graph) {
        colonelWest =  putEntity(graph, "colonelWest", person, key.getName());
        Nono =  putEntity(graph, "Nono", country, key.getName());
        America =  putEntity(graph, "America", country, key.getName());
        Tomahawk =  putEntity(graph, "Tomahawk", rocket, key.getName());

        putResource(colonelWest, nationality, "American");
        putResource(Tomahawk, propulsion, "gsp");
    }

    @Override
    protected void buildRelations(GraknGraph graph) {
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
    protected void buildRules(GraknGraph graph) {
        RuleType inferenceRule = graph.admin().getMetaRuleInference();

        //R1: "It is a crime for an American to sell weapons to hostile nations"
        Pattern R1_LHS = Graql.and(
                graph.graql().parsePatterns("$x isa person;$x has nationality 'American';" +
                "$y isa weapon;" +
                "$z isa country;$z has alignment 'hostile';" +
                "(seller: $x, transaction-item: $y, buyer: $z) isa transaction;"));

        Pattern R1_RHS = Graql.and(graph.graql().parsePatterns("$x isa criminal;"));
        inferenceRule.putRule(R1_LHS, R1_RHS);

        //R2: "Missiles are a kind of a weapon"
        Pattern R2_LHS = Graql.and(graph.graql().parsePatterns("$x isa missile;"));
        Pattern R2_RHS = Graql.and(graph.graql().parsePatterns("$x isa weapon;"));
        inferenceRule.putRule(R2_LHS, R2_RHS);

        //R3: "If a country is an enemy of America then it is hostile"
        Pattern R3_LHS = Graql.and(
                graph.graql().parsePatterns("$x isa country;" +
                "($x, $y) isa is-enemy-of;" +
                "$y isa country;$y has name 'America';"));
        Pattern R3_RHS = Graql.and(graph.graql().parsePatterns("$x has alignment 'hostile';"));
        inferenceRule.putRule(R3_LHS, R3_RHS);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        Pattern R4_LHS = Graql.and(graph.graql().parsePatterns("$x isa rocket;$x has propulsion 'gsp';"));
        Pattern R4_RHS = Graql.and(graph.graql().parsePatterns("$x isa missile;"));
        inferenceRule.putRule(R4_LHS, R4_RHS);

        Pattern R5_LHS = Graql.and(
                graph.graql().parsePatterns("$x isa person;" +
                "$y isa country;" +
                "$z isa weapon;" +
                "($x, $y) isa is-paid-by;" +
                "($y, $z) isa owns;"));

        Pattern R5_RHS = Graql.and(graph.graql().parsePatterns("(seller: $x, buyer: $y, transaction-item: $z) isa transaction;"));
        inferenceRule.putRule(R5_LHS, R5_RHS);
    }
}
