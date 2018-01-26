/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Pattern;
import ai.grakn.test.rule.SampleKBContext;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class CWKB extends TestKB {

    private static AttributeType<String> key;

    private static EntityType person;
    private static EntityType rocket;
    private static EntityType country;

    private static AttributeType<String> propulsion;
    private static AttributeType<String> nationality;

    private static RelationshipType isEnemyOf;
    private static RelationshipType isPaidBy;
    private static RelationshipType owns;

    private static Role enemySource, enemyTarget;
    private static Role owner, ownedItem;
    private static Role payee, payer;

    private static Thing colonelWest, Nono, America, Tomahawk;

    public static SampleKBContext context() {
        return new CWKB().makeContext();
    }

    @Override
    protected void buildSchema(GraknTx tx) {
        key = tx.putAttributeType("name", AttributeType.DataType.STRING);

        //Resources
        nationality = tx.putAttributeType("nationality", AttributeType.DataType.STRING);
        propulsion = tx.putAttributeType("propulsion", AttributeType.DataType.STRING);
        AttributeType<String> alignment = tx.putAttributeType("alignment", AttributeType.DataType.STRING);

        //Roles
        owner = tx.putRole("item-owner");
        ownedItem = tx.putRole("owned-item");
        payee = tx.putRole("payee");
        payer = tx.putRole("payer");
        enemySource = tx.putRole("enemy-source");
        enemyTarget = tx.putRole("enemy-target");
        Role seller = tx.putRole("seller");
        Role buyer = tx.putRole("buyer");
        Role transactionItem = tx.putRole("transaction-item");

        EntityType baseEntity = tx.putEntityType("baseEntity")
                .attribute(key);

        //Entitites
        person = tx.putEntityType("person")
                .sup(baseEntity)
                .plays(seller)
                .plays(payee)
                .attribute(nationality);

        tx.putEntityType("criminal")
                .sup(person);

        EntityType weapon = tx.putEntityType("weapon")
                .sup(baseEntity)
                .plays(transactionItem)
                .plays(ownedItem);

        rocket = tx.putEntityType("rocket")
                .sup(weapon)
                .attribute(propulsion);

        tx.putEntityType("missile")
                .sup(weapon) //sup(rocket)?
                .attribute(propulsion);

        country = tx.putEntityType("country")
                .sup(baseEntity)
                .plays(buyer)
                .plays(owner)
                .plays(enemyTarget)
                .plays(payer)
                .plays(enemySource)
                .attribute(alignment);

        //Relations
        owns = tx.putRelationshipType("owns")
                .relates(owner)
                .relates(ownedItem);

        isEnemyOf = tx.putRelationshipType("is-enemy-of")
                .relates(enemySource)
                .relates(enemyTarget);

        tx.putRelationshipType("transaction")
                .relates(seller)
                .relates(buyer)
                .relates(transactionItem);

        isPaidBy = tx.putRelationshipType("is-paid-by")
                .relates(payee)
                .relates(payer);
    }

    @Override
    protected void buildInstances(GraknTx tx) {
        colonelWest =  putEntityWithResource(tx, "colonelWest", person, key.getLabel());
        Nono =  putEntityWithResource(tx, "Nono", country, key.getLabel());
        America =  putEntityWithResource(tx, "America", country, key.getLabel());
        Tomahawk =  putEntityWithResource(tx, "Tomahawk", rocket, key.getLabel());

        putResource(colonelWest, nationality, "American");
        putResource(Tomahawk, propulsion, "gsp");
    }

    @Override
    protected void buildRelations() {
        //Enemy(Nono, America)
        isEnemyOf.addRelationship()
                .addRolePlayer(enemySource, Nono)
                .addRolePlayer(enemyTarget, America);

        //Owns(Nono, Missile)
        owns.addRelationship()
                .addRolePlayer(owner, Nono)
                .addRolePlayer(ownedItem, Tomahawk);

        //isPaidBy(West, Nono)
        isPaidBy.addRelationship()
                .addRolePlayer(payee, colonelWest)
                .addRolePlayer(payer, Nono);
    }

    @Override
    protected void buildRules(GraknTx tx) {
        //R1: "It is a crime for an American to sell weapons to hostile nations"
        Pattern R1_LHS = tx.graql().parser().parsePattern("{" +
                        "$x isa person;$x has nationality 'American';" +
                        "$y isa weapon;" +
                        "$z isa country;$z has alignment 'hostile';" +
                        "(seller: $x, transaction-item: $y, buyer: $z) isa transaction;}");

        Pattern R1_RHS = tx.graql().parser().parsePattern("{$x isa criminal;}");
        tx.putRule("R1: It is a crime for an American to sell weapons to hostile nations" , R1_LHS, R1_RHS);

        //R2: "Missiles are a kind of a weapon"
        Pattern R2_LHS = tx.graql().parser().parsePattern("{$x isa missile;}");
        Pattern R2_RHS = tx.graql().parser().parsePattern("{$x isa weapon;}");
        tx.putRule("R2: Missiles are a kind of a weapon\"" , R2_LHS, R2_RHS);

        //R3: "If a country is an enemy of America then it is hostile"
        Pattern R3_LHS = tx.graql().parser().parsePattern("{$x isa country;" +
                "($x, $y) isa is-enemy-of;" +
                "$y isa country;$y has name 'America';}");
        Pattern R3_RHS = tx.graql().parser().parsePattern("{$x has alignment 'hostile';}");
        tx.putRule("R3: If a country is an enemy of America then it is hostile" , R3_LHS, R3_RHS);

        //R4: "If a rocket is self-propelled and guided, it is a missile"
        Pattern R4_LHS = tx.graql().parser().parsePattern("{$x isa rocket;$x has propulsion 'gsp';}");
        Pattern R4_RHS = tx.graql().parser().parsePattern("{$x isa missile;}");
        tx.putRule("R4: If a rocket is self-propelled and guided, it is a missile" , R4_LHS, R4_RHS);

        Pattern R5_LHS = tx.graql().parser().parsePattern("{$x isa person;" +
                "$y isa country;" +
                "$z isa weapon;" +
                "($x, $y) isa is-paid-by;" +
                "($y, $z) isa owns;}");

        Pattern R5_RHS = tx.graql().parser().parsePattern("{(seller: $x, buyer: $y, transaction-item: $z) isa transaction;}");
        tx.putRule("R5: If a country pays a person and that country now owns a weapon then the person has sold the country a weapon" , R5_LHS, R5_RHS);
    }
}
