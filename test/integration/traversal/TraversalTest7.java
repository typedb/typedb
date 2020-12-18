/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.traversal;

import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlQuery;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Session.Type.SCHEMA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static graql.lang.Graql.parseQueries;
import static graql.lang.Graql.parseQuery;

public class TraversalTest7 {
    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("simulation");
    private static String database = "simulation";
    private static RocksGrakn grakn;

    @Test
    public void test() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);

        try (RocksSession session = grakn.session(database, SCHEMA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final GraqlDefine query = parseQuery(schema());
                transaction.query().define(query);
                transaction.commit();
            }
        }

        try (RocksSession session = grakn.session(database, DATA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final Stream<GraqlQuery> queries = parseQueries(data());
                queries.forEach(query -> {
                    if (query instanceof GraqlInsert) {
                        transaction.query().insert(query.asInsert());
                    } else if (query instanceof GraqlMatch) {
                        transaction.query().match(query.asMatch());
                    }
                });
                transaction.commit();
            }
        }
    }

    @AfterClass
    public static void after() {
        grakn.close();
    }

    private static String data() {
        return "insert $x isa continent, has location-name \"Europe\";\n" +
                "match $c isa continent, has location-name \"Europe\";\n" +
                "insert\n" +
                "$x isa country, has location-name \"United Kingdom\";\n" +
                "(superior: $c, subordinate: $x) isa location-hierarchy;\n" +
                "match $c isa country, has location-name \"United Kingdom\";\n" +
                "insert\n" +
                "$x isa city, has location-name \"London\";\n" +
                "(superior: $c, subordinate: $x) isa location-hierarchy;\n" +
                "match $country isa country, has location-name \"United Kingdom\";\n" +
                "insert\n" +
                "$company isa company, has company-name \"IncreasingBullfinch-1448614281\", has company-number 1448614281;\n" +
                "$incorporation (incorporated: $company, incorporating: $country) isa incorporation, has date-of-incorporation 0001-01-01T00:00;\n" +
                "match $continent isa continent, has location-name \"Europe\";\n" +
                "insert\n" +
                "$product isa product, has product-barcode -261994687, has product-name \"dZSYNLmTqLsCRcg\", has product-description \"QOG9Zsdap5B39T0M04wGvNnu3c4dHCBUO7exyPcfhtAgKGn6fto6D8sAAKUv1gfzVA66QTHebHgrexSXBroo1e3SrqgncBpIh\";\n" +
                "$produced-in (product: $product, continent: $continent) isa produced-in;\n" +
                "match\n" +
                "$country isa country, has location-name \"United Kingdom\";\n" +
                "$company isa company, has company-number $company-number;\n" +
                "$incorporation (incorporated: $company, incorporating: $country) isa incorporation;\n" +
                "match\n" +
                "$continent isa continent, has location-name \"Europe\";\n" +
                "$product isa product, has product-barcode $product-barcode;\n" +
                "$produced-in (product: $product, continent: $continent) isa produced-in;\n" +
                "match\n" +
                "$product isa product, has product-barcode -261994687;\n" +
                "$c-buyer isa company, has company-number 1448614281;\n" +
                "$c-seller isa company, has company-number 1448614281;\n" +
                "$country isa country, has location-name \"United Kingdom\";\n" +
                "insert\n" +
                "$transaction (seller: $c-seller, buyer: $c-buyer, merchandise: $product) isa transaction, has value 8463.54777, has product-quantity 80, has is-taxable true;\n" +
                "$locates (location: $country, located: $transaction) isa locates;\n";
    }

    private static String schema() {
        return "define\n" +
                "name sub attribute, abstract,\n" +
                "    value string;\n" +
                "location-name sub name;\n" +
                "location sub entity,\n" +
                "    abstract,\n" +
                "    owns location-name @key,\n" +
                "    plays location-hierarchy:superior,\n" +
                "    plays location-hierarchy:subordinate,\n" +
                "    plays locates:location;\n" +
                "location-hierarchy sub relation,\n" +
                "    relates superior,\n" +
                "    relates subordinate;\n" +
                "locates sub relation,\n" +
                "    relates location,\n" +
                "    relates located;\n" +
                "city sub location;\n" +
                "country sub location,\n" +
                "    plays incorporation:incorporating;\n" +
                "continent sub location,\n" +
                "    plays produced-in:continent;\n" +
                "date-of-event sub attribute,\n" +
                "    abstract,\n" +
                "    value datetime;\n" +
                "date-of-incorporation sub date-of-event;\n" +
                "organisation-name sub name, abstract;\n" +
                "company-name sub organisation-name;\n" +
                "company-number sub attribute,\n" +
                "    value long;\n" +
                "product-name sub name;\n" +
                "value sub attribute,\n" +
                "    value double;\n" +
                "organisation sub entity;\n" +
                "company sub organisation,\n" +
                "    owns company-name @key,\n" +
                "    owns company-number @key,\n" +
                "    plays transaction:seller,\n" +
                "    plays transaction:buyer,\n" +
                "    plays incorporation:incorporated;\n" +
                "incorporation sub relation,\n" +
                "    owns date-of-incorporation,\n" +
                "    relates incorporated,\n" +
                "    relates incorporating;\n" +
                "transaction sub relation,\n" +
                "    relates buyer,\n" +
                "    relates seller,\n" +
                "    relates merchandise,\n" +
                "    owns value,\n" +
                "    owns product-quantity,\n" +
                "    owns is-taxable,\n" +
                "    plays locates:located;\n" +
                "product sub entity,\n" +
                "    owns product-barcode @key,\n" +
                "    owns product-name,\n" +
                "    owns product-description,\n" +
                "    plays transaction:merchandise,\n" +
                "    plays produced-in:product;\n" +
                "produced-in sub relation,\n" +
                "    relates product,\n" +
                "    relates continent;\n" +
                "identifier sub attribute, abstract,\n" +
                "    value long;\n" +
                "product-barcode sub identifier;\n" +
                "description sub attribute, abstract,\n" +
                "    value string;\n" +
                "product-description sub description;\n" +
                "product-quantity sub attribute,\n" +
                "    value long;\n" +
                "is-taxable sub attribute,\n" +
                "    value boolean;\n";
    }
}
