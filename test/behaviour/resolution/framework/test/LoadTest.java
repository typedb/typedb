/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.test.behaviour.resolution.framework.test;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typeql.lang.TypeQL;


public class LoadTest {

    // TODO: These should use TypeQL builder to make them robust to change
    static void loadEmployableExample(TypeDB typeDB, String databaseName) {
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                String schema = "" +
                        "define\n" +
                        "employable sub attribute,\n" +
                        "    value boolean;" +
                        "animal sub entity," +
                        "  owns employable;\n" +
                        "person sub animal;" +
                        "dog sub animal;" +
                        "rule people-are-employable:\n" +
                        "when {\n" +
                        "    $p isa person;\n" +
                        "} then {\n" +
                        "    $p has employable true;\n" +
                        "};";
                tx.query().define(TypeQL.parseQuery(schema).asDefine());
                tx.commit();
            }
        }
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.parseQuery("insert $p isa person; $d isa dog;").asInsert());
                tx.commit();
            }
        }
    }

    // TODO: These should use TypeQL builder to make them robust to change
    static void loadTransitivityExample(TypeDB typeDB, String databaseName) {
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                String schema = "" +
                        "define\n" +
                        "name sub attribute,\n" +
                        "    value string;\n" +
                        "location sub entity,\n" +
                        "    abstract,\n" +
                        "    owns name @key,\n" +
                        "    plays location-hierarchy:superior,\n" +
                        "    plays location-hierarchy:subordinate;\n" +
                        "area sub location;\n" +
                        "city sub location;\n" +
                        "country sub location;\n" +
                        "continent sub location;\n" +
                        "location-hierarchy sub relation,\n" +
                        "    relates superior,\n" +
                        "    relates subordinate;\n" +
                        "rule location-hierarchy-transitivity:\n" +
                        "when {\n" +
                        "    (superior: $a, subordinate: $b) isa location-hierarchy;\n" +
                        "    (superior: $b, subordinate: $c) isa location-hierarchy;\n" +
                        "} then {\n" +
                        "    (superior: $a, subordinate: $c) isa location-hierarchy;\n" +
                        "};";
                tx.query().define(TypeQL.parseQuery(schema).asDefine());
                tx.commit();
            }
        }
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.parseQuery(
                        "insert\n" +
                                "$ar isa area, has name \"King's Cross\";\n" +
                                "$cit isa city, has name \"London\";\n" +
                                "$cntry isa country, has name \"UK\";\n" +
                                "$cont isa continent, has name \"Europe\";\n" +
                                "(superior: $cont, subordinate: $cntry) isa location-hierarchy;\n" +
                                "(superior: $cntry, subordinate: $cit) isa location-hierarchy;\n" +
                                "(superior: $cit, subordinate: $ar) isa location-hierarchy;"
                ).asInsert());
                tx.commit();
            }
        }
    }

    static void loadBasicRecursionExample(TypeDB typeDB, String databaseName) {
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                String schema = "define\n" +
                        "name sub attribute,\n" +
                        "    value string;\n" +
                        "is-liable sub attribute,\n" +
                        "    value boolean;\n" +
                        "company-id sub attribute,\n" +
                        "    value long;\n" +
                        "company sub entity,\n" +
                        "    owns company-id @key,\n" +
                        "    owns name,\n" +
                        "    owns is-liable;\n" +
                        "rule company-has-name:\n" +
                        "when {\n" +
                        "    $c1 isa company;\n" +
                        "} then {\n" +
                        "    $c1 has name \"the-company\";\n" +
                        "};\n" +
                        "rule company-is-liable:\n" +
                        "when {\n" +
                        "    $c2 isa company, has name $n2; $n2 \"the-company\";\n" +
                        "} then {\n" +
                        "    $c2 has is-liable true;\n" +
                        "};";
                tx.query().define(TypeQL.parseQuery(schema).asDefine());
                tx.commit();
            }
        }
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.parseQuery("insert $x isa company, has company-id 0;").asInsert());
                tx.commit();
            }
        }
    }

    static void loadComplexRecursionExample(TypeDB typeDB, String databaseName) {
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                String schema = "define\n" +
                        "currency sub attribute,\n" +
                        "    value string;\n" +
                        "country-name sub attribute,\n" +
                        "    value string;\n" +
                        "city-name sub attribute,\n" +
                        "    value string;\n" +
                        "transaction sub entity,\n" +
                        "    owns currency,\n" +
                        "    plays locates:located;\n" +
                        "locates sub relation,\n" +
                        "    relates location,\n" +
                        "    relates located;\n" +
                        "location-hierarchy sub relation,\n" +
                        "    relates superior,\n" +
                        "    relates subordinate;\n" +
                        "location sub entity,\n" +
                        "    abstract,\n" +
                        "    plays location-hierarchy:superior,\n" +
                        "    plays location-hierarchy:subordinate;\n" +
                        "country sub location,\n" +
                        "    owns country-name @key,\n" +
                        "    owns currency,\n" +
                        "    plays locates:location;\n" +
                        "city sub location,\n" +
                        "    owns city-name @key,\n" +
                        "    plays locates:location;\n" +
                        "rule locates-is-transitive:\n" +
                        "when {\n" +
                        "    $city isa city;\n" +
                        "    $country isa country;\n" +
                        "    $lh(superior: $country, subordinate: $city) isa location-hierarchy;\n" +
                        "    $l1(located: $transaction, location: $city) isa locates;\n" +
                        "} then {\n" +
                        "    (located: $transaction, location: $country) isa locates;\n" +
                        "};\n" +
                        "rule transaction-currency-is-that-of-the-country:\n" +
                        "when {\n" +
                        "    $transaction isa transaction;\n" +
                        "    $locates(located: $transaction, location: $country) isa locates;\n" +
                        "    $country isa country, has currency $currency;\n" +
                        "} then {\n" +
                        "    $transaction has currency $currency;\n" +
                        "};";
                tx.query().define(TypeQL.parseQuery(schema).asDefine());
                tx.commit();
            }
        }
        try (TypeDB.Session session = typeDB.session(databaseName, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.parseQuery("" +
                                "insert\n" +
                                "$transaction isa transaction;\n" +
                                "$locates(located: $transaction, location: $city) isa locates;\n" +
                                "$country isa country, has currency \"GBP\", has country-name \"UK\";\n" +
                                "$city isa city, has city-name \"London\";\n" +
                                "$lh(hierarchy_superior: $country, hierarchy_subordinate: $city) isa location-hierarchy;"
                ).asInsert());
                tx.commit();
            }
        }
    }
}
