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
 */

package grakn.core.graql.reasoner;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.graph.GeoGraph;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import org.apache.commons.collections.CollectionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static graql.lang.Graql.var;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class GeoInferenceIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static Session geoGraphSession;

    @BeforeClass
    public static void loadContext(){
        geoGraphSession = server.sessionWithNewKeyspace();
        GeoGraph geoGraph = new GeoGraph(geoGraphSession);
        geoGraph.load();
    }

    @AfterClass
    public static void closeSession(){
        geoGraphSession.close();
    }



    @Test
    public void testEntitiesLocatedInThemselves(){
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            String queryString = "match (geo-entity: $x, entity-location: $x) isa is-located-in; get;";

            GraqlGet query = Graql.parse(queryString).asGet();
            List<ConceptMap> answers = tx.execute(query);
            assertThat(answers, empty());
        }
    }

    @Test
    public void testTransitiveQuery_withGuards() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            String queryString = "match " +
                    "$x isa university;$x has name $name;" +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$y isa country;$y has name 'Poland';" +
                    "get $x, $name;";
            String explicitQuery = "match " +
                    "$x isa university;$x has name $name;" +
                    "{$x has name 'University-of-Warsaw';} or {$x has name'Warsaw-Polytechnics';}; get;";

            assertCollectionsNonTriviallyEqual(
                    tx.execute(Graql.parse(queryString).asGet()),
                    tx.execute(Graql.parse(explicitQuery).asGet(), false)
            );
        }
    }

    @Test
    public void testTransitiveQuery_withGuards_noRoles() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            String queryString = "match " +
                    "$z1 isa university;$z1 has name $name;" +
                    "($z1, $z2) isa is-located-in;" +
                    "$z2 isa country;$z2 has name 'Poland';" +
                    "get $z1, $name;";
            String queryString2 = "match " +
                    "$z2 isa university;$z2 has name $name;" +
                    "($z1, $z2) isa is-located-in;" +
                    "$z1 isa country;$z1 has name 'Poland';" +
                    "get $z2, $name;";
            String explicitQuery = "match " +
                    "$z1 isa university;$z1 has name $name;" +
                    "{$z1 has name 'University-of-Warsaw';} or {$z1 has name'Warsaw-Polytechnics';}; get;";
            String explicitQuery2 = "match " +
                    "$z2 isa university;$z2 has name $name;" +
                    "{$z2 has name 'University-of-Warsaw';} or {$z2 has name'Warsaw-Polytechnics';}; get;";

            assertCollectionsNonTriviallyEqual(
                    tx.execute(Graql.parse(queryString).asGet()),
                    tx.execute(Graql.parse(explicitQuery).asGet(), false)
            );
            assertCollectionsNonTriviallyEqual(
                    tx.execute(Graql.parse(queryString2).asGet()),
                    tx.execute(Graql.parse(explicitQuery2).asGet(), false)
            );
        }
    }

    @Test
    public void testTransitiveQuery_withSpecificResource() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            String queryString = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$y has name 'Poland'; get;";

            String queryString2 = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$y has name 'Europe'; get;";

            Concept poland = getConcept(tx, "name", "Poland");
            Concept europe = getConcept(tx, "name", "Europe");

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            answers.forEach(ans -> assertEquals(2, ans.size()));
            answers.forEach(ans -> assertEquals(poland.id().getValue(), ans.get("y").id().getValue()));
            assertEquals(6, answers.size());


            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            answers2.forEach(ans -> assertEquals(2, ans.size()));
            answers2.forEach(ans -> assertEquals(europe.id().getValue(), ans.get("y").id().getValue()));
            assertEquals(21, answers2.size());
        }
    }

    @Test
    public void testTransitiveQuery_withSpecificResource_noRoles() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            Concept masovia = getConcept(tx, "name", "Masovia");
            String queryString = "match " +
                    "($x, $y) isa is-located-in;" +
                    "$y has name 'Masovia'; get;";
            String explicitString = "match " +
                    "{ (geo-entity: $x, entity-location: $y) isa is-located-in; } or " +
                    "{ (geo-entity: $y, entity-location: $x) isa is-located-in; };" +
                    "$y has name 'Masovia'; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            List<ConceptMap> explicitAnswers = tx.execute(Graql.parse(explicitString).asGet());
            answers.forEach(ans -> assertEquals(2, ans.size()));
            answers.forEach(ans -> assertEquals(masovia.id().getValue(), ans.get("y").id().getValue()));
            assertEquals(5, answers.size());
            assertCollectionsNonTriviallyEqual(answers, explicitAnswers);
        }
    }

    @Test
    public void testTransitiveQuery_withSubstitution() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            Concept poland = getConcept(tx, "name", "Poland");
            Concept europe = getConcept(tx, "name", "Europe");
            String queryString = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$y id " + poland.id().getValue() + "; get;";

            String queryString2 = "match " +
                    "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                    "$y id " + europe.id().getValue() + "; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            answers.forEach(ans -> assertEquals(2, ans.size()));
            answers.forEach(ans -> assertEquals(ans.get("y").id().getValue(), poland.id().getValue()));
            assertEquals(6, answers.size());

            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            answers2.forEach(ans -> assertEquals(2, ans.size()));
            answers2.forEach(ans -> assertEquals(ans.get("y").id().getValue(), europe.id().getValue()));
            assertEquals(tx.execute(Graql.parse("match $x isa entity; get;").asGet()).size() - 1, answers2.size());
        }
    }

    @Test
    public void testTransitiveQuery_withSubstitution_noRoles() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            

            Concept masovia = getConcept(tx, "name", "Masovia");
            String queryString = "match " +
                    "($x, $y) isa is-located-in;" +
                    "$y id " + masovia.id().getValue() + "; get;";

            String queryString2 = "match " +
                    "{ (geo-entity: $x, entity-location: $y) isa is-located-in; } or " +
                    "{ (geo-entity: $y, entity-location: $x) isa is-located-in; };" +
                    "$y id " + masovia.id().getValue() + "; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            answers.forEach(ans -> assertEquals(2, ans.size()));
            answers.forEach(ans -> assertEquals(masovia.id().getValue(), ans.get("y").id().getValue()));
            assertEquals(5, answers.size());
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            assertCollectionsNonTriviallyEqual(answers, answers2);
        }
    }

    @Test
    public void testTransitiveQuery_withSubstitution_variableRoles() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            Concept masovia = getConcept(tx, "name", "Masovia");
            String queryString = "match " +
                    "($r1: $x, $r2: $y) isa is-located-in;" +
                    "$y id " + masovia.id().getValue() + "; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            answers.forEach(ans -> assertEquals(ans.size(), 4));
            answers.forEach(ans -> assertEquals(ans.get("y").id().getValue(), masovia.id().getValue()));
            assertEquals(20, answers.size());
        }
    }

    @Test
    public void testTransitiveQuery_Closure() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(51, answers.size());
        }
    }

    @Test
    public void testTransitiveQuery_Closure_NoRoles() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            String queryString = "match ($x, $y) isa is-located-in; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(102, answers.size());
        }
    }

    @Test
    public void testTransitiveQuery_Closure_variableRoles() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            String queryString = "match ($r1: $x, $r2: $y) isa is-located-in; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            answers.forEach(ans -> assertEquals(4, ans.size()));
            assertEquals(408, answers.size());
        }
    }

    @Test
    public void testTransitiveQuery_variableRoles_withSubstitution_withRelationVar() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            Concept masovia = getConcept(tx, "name", "Masovia");
            String queryString = "match " +
                    "$x ($r1: $x1, $r2: $x2) isa is-located-in;" +
                    "$x2 id " + masovia.id().getValue() + "; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(20, answers.size());
            answers.forEach(ans -> assertEquals(5, ans.size()));
        }
    }

    @Test
    public void testTransitiveQuery_Closure_variableSpecificRoles() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            Statement rolePattern = var()
                    .rel(var("r1").type("geo-entity"), var("x"))
                    .rel(var("r2").type("entity-location"), var("y"));

            List<ConceptMap> answers = tx.execute(Graql.match(rolePattern).get());

            answers.forEach(ans -> assertEquals(4, ans.size()));
            assertEquals(51, answers.size());
        }
    }

    @Test
    public void testTransitiveQuery_Closure_singleVariableRole() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            String queryString = "match ($x, $r2: $y) isa is-located-in; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            answers.forEach(ans -> assertEquals(3, ans.size()));
            assertEquals(204, answers.size());
        }
    }

    @Test
    public void testTransitiveQuery_Closure_singleVariableRole_withSubstitution() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            Concept masovia = getConcept(tx, "name", "Masovia");
            String queryString = "match " +
                    "($x, $r2: $y) isa is-located-in;" +
                    "$y id " + masovia.id().getValue() + "; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            answers.forEach(ans -> assertEquals(3, ans.size()));
            answers.forEach(ans -> assertEquals(masovia.id().getValue(), ans.get("y").id().getValue()));
            assertEquals(10, answers.size());
        }
    }

    @Test
    public void testTransitiveQuery_Closure_withRelationVar() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            String queryString = "match $x (geo-entity: $x1, entity-location: $x2) isa is-located-in; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(51, answers.size());
        }
    }

    @Test
    public void testRelationVarQuery_Closure_withAndWithoutRelationPlayers() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            String queryString = "match $x isa is-located-in; get;";
            String queryString2 = "match $x ($x1, $x2) isa is-located-in;get $x;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());

            assertEquals(51, answers.size());
            assertEquals(51, answers2.size());
        }
    }

    @Test
    public void testLazy() {
        try (Transaction tx = geoGraphSession.writeTransaction()) {
            
            String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get; limit 1;";
            String queryString2 = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get; limit 22;";
            String queryString3 = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            List<ConceptMap> answers3 = tx.execute(Graql.parse(queryString3).asGet());
            assertTrue(answers3.containsAll(answers));
            assertTrue(answers3.containsAll(answers2));
        }
    }

    private Concept getConcept(Transaction graph, String typeName, String val){
        return graph.stream(Graql.match((Pattern) var("x").has(typeName, val)).get("x"))
                .map(ans -> ans.get("x")).findAny().orElse(null);
    }

    private static <T> void assertCollectionsNonTriviallyEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue(CollectionUtils.isEqualCollection(c1, c2));
    }
}
