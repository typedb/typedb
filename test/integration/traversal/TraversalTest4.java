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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.traversal.common.Predicate;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.procedure.GraphProcedure;
import grakn.core.traversal.procedure.ProcedureVertex;
import graql.lang.query.GraqlMatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static graql.lang.Graql.parseQuery;
import static graql.lang.common.GraqlToken.Predicate.Equality.EQ;
import static org.junit.Assert.assertTrue;

public class TraversalTest4 {
    private static Path directory = Paths.get("/Users/haikalpribadi/Workspace/git/graknlabs/grakn/data/");
    private static String database = "world";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void before() {
        grakn = RocksGrakn.open(directory);
        session = grakn.session(database, DATA);
    }

    @AfterClass
    public static void after() {
        session.close();
        grakn.close();
    }

    @Test
    public void test_simulation_traversal_repeat() {
        int success = 0, fail = 0;
        for (int i = 0; i < 100; i++) {

            try {
                test_simulation_traversal();
                success++;
                System.out.println(String.format("Success: %s, Fail: %s", success, fail));
            } catch (Throwable error) {
                error.printStackTrace();
                fail++;
                System.out.println(String.format("Success: %s, Fail: %s", success, fail));
                break;
            }
        }
    }

    @Test
    public void test_simulation_traversal() {
        try (RocksTransaction transaction = session.transaction(READ)) {
            GraqlMatch query = parseQuery(
                    "match\n" +
                            "$person isa person, has gender \"male\", has email $email, has date-of-birth $date-of-birth;\n" +
                            "$date-of-birth <= 0004-01-01T00:00;\n" +
                            "$location has location-name $location-name;\n" +
                            "$location-name = 'Berlin';\n" +
//                            "(child: $person, place-of-birth: $location) isa born-in;\n" +
                            "not { (child: $person, place-of-birth: $location) isa born-in; };\n" +
                            "get $email, $location-name;"
            ).asMatch();
            ResourceIterator<ConceptMap> answers = transaction.query().match(query, false);
            assertTrue(answers.hasNext());
            int count = 0;
            while (answers.hasNext()) {
                count++;
                ConceptMap answer = answers.next();
                System.out.println(answer.get("email").asAttribute().asString().getValue());
                System.out.println(answer.get("location-name").asAttribute().asString().getValue());
            }
            System.out.println("Total: " + count);
        }
    }

    @Test
    public void test_simulation_traversal_procedure() {
        GraphProcedure.Builder proc = GraphProcedure.builder(13);
        Traversal.Parameters params = new Traversal.Parameters();

        ProcedureVertex.Thing _0 = proc.anonymousThing(0);
        ProcedureVertex.Type _continent = proc.labelledType("continent");
        ProcedureVertex.Type _location_name = proc.labelledType("location-name");
        ProcedureVertex.Type _produced_in = proc.labelledType("produced-in");
        ProcedureVertex.Type _produced_in_continent = proc.labelledType("produced-in:continent");
        ProcedureVertex.Type _produced_in_product = proc.labelledType("produced-in:product");
        ProcedureVertex.Type _product = proc.labelledType("product");
        ProcedureVertex.Type _product_barcode = proc.labelledType("product-barcode");
        ProcedureVertex.Thing continent = proc.namedThing("continent");
        ProcedureVertex.Thing produced_in = proc.namedThing("produced-in");
        ProcedureVertex.Thing product = proc.namedThing("product");
        ProcedureVertex.Thing product_barcode = proc.namedThing("product-barcode", true);
        ProcedureVertex.Thing produced_in_product = proc.scopedThing(produced_in, _produced_in_product, product, 1);
        ProcedureVertex.Thing produced_in_continent = proc.scopedThing(produced_in, _produced_in_continent, continent, 1);

        proc.setLabel(_continent, "continent");
        proc.setLabel(_location_name, "location-name");
        proc.setLabel(_produced_in, "produced-in");
        proc.setLabel(_produced_in_continent, "produced-in:continent");
        proc.setLabel(_produced_in_product, "produced-in:product");
        proc.setLabel(_product, "product");
        proc.setLabel(_product_barcode, "product-barcode");

        Predicate.Value.SubString predicate = Predicate.Value.SubString.of(EQ);
        params.pushValue(_0.id().asVariable(), predicate, new Traversal.Parameters.Value("Asia"));
        proc.setPredicate(_0, predicate);

        proc.backwardHas(1, product_barcode, product);
        proc.forwardIsa(2, product, _product, true);
        proc.forwardIsa(3, product_barcode, _product_barcode, true);
        proc.forwardPlaying(4, product, produced_in_product);
        proc.forwardIsa(5, produced_in_product, _produced_in_product, true);
        proc.backwardRelating(6, produced_in_product, produced_in);
        proc.forwardIsa(7, produced_in, _produced_in, true);
        proc.forwardRelating(8, produced_in, produced_in_continent);
        proc.backwardPlaying(9, produced_in_continent, continent);
        proc.forwardHas(10, continent, _0);
        proc.forwardIsa(11, _0, _location_name, true);
        proc.forwardIsa(12, produced_in_continent, _produced_in_continent, true);
        proc.forwardIsa(13, continent, _continent, true);

        try (RocksTransaction transaction = session.transaction(READ)) {
            ResourceIterator<VertexMap> vertices = transaction.traversal().iterator(proc.build(), params);
            ResourceIterator<ConceptMap> answers = transaction.concepts().conceptMaps(vertices);
            assertTrue(answers.hasNext());
            while (answers.hasNext()) {
                ConceptMap answer = answers.next();
                System.out.println(answer);
            }
        }
    }
}
