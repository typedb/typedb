/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.reasoner.graph.GeoGraph;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.query.pattern.Pattern.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicQueryIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl materialisationTestSession;
    private static SessionImpl geoGraphSession;

    private static void loadFromFile(String fileName, Session session) {
        try {
            InputStream inputStream = AtomicQueryIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/" + fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            Graql.parseList(s).forEach(tx::execute);
            tx.commit();
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void loadContext() {
        materialisationTestSession = server.sessionWithNewKeyspace();
        loadFromFile("materialisationTest.gql", materialisationTestSession);
        geoGraphSession = server.sessionWithNewKeyspace();
        GeoGraph geoGraph = new GeoGraph(geoGraphSession);
        geoGraph.load();
    }

    @AfterClass
    public static void closeSession() {
        materialisationTestSession.close();
        geoGraphSession.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhenConstructingNonAtomicQuery_ExceptionIsThrown() {
        try (TransactionOLTP tx = geoGraphSession.transaction(Transaction.Type.WRITE)) {
            String patternString = "{$x isa university;$y isa country;($x, $y) isa is-located-in;($y, $z) isa is-located-in;}";
            ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(conjunction(patternString), tx);
        }
    }

    @Test(expected = GraqlQueryException.class)
    public void testWhenCreatingQueryWithNonexistentType_ExceptionIsThrown() {
        try (TransactionOLTP tx = geoGraphSession.transaction(Transaction.Type.WRITE)) {
            String patternString = "{$x isa someType;}";
            ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(patternString), tx);
        }
    }

    @Test
    public void testWhenMaterialising_MaterialisedInformationIsPresentInGraph() {
        TransactionOLTP tx = geoGraphSession.transaction(Transaction.Type.WRITE);
                String explicitGetQueryStr = "match (geo-entity: $x, entity-location: $y) isa is-located-in;$x has name 'Warsaw';$y has name 'Poland'; get;";
        GetQuery explicitGetQuery = Graql.parse(explicitGetQueryStr);
        assertFalse(tx.stream(explicitGetQuery,false).iterator().hasNext());

        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        Conjunction<Statement> pattern = conjunction(patternString);
        List<ConceptMap> answers = new ArrayList<>();

        answers.add(new ConceptMap(
                ImmutableMap.of(
                        var("x"), getConceptByResourceValue(tx, "Warsaw"),
                        var("y"), getConceptByResourceValue(tx, "Poland")))
        );
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, tx);

        assertFalse(tx.stream(explicitGetQuery, false).iterator().hasNext());
        answers.forEach(atomicQuery::materialise);
        assertTrue(tx.stream(explicitGetQuery, false).iterator().hasNext());
        tx.close();
    }

    @Test
    public void testWhenMaterialisingEntity_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        TransactionOLTP tx = materialisationTestSession.transaction(Transaction.Type.WRITE);
        ReasonerAtomicQuery entityQuery = ReasonerQueries.atomic(conjunction("$x isa newEntity"), tx);
        assertEquals(entityQuery.materialise(new ConceptMap()).findFirst().orElse(null).get("x").asEntity().isInferred(), true);
        tx.close();
    }

    @Test
    public void testWhenMaterialisingResources_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        TransactionOLTP tx = materialisationTestSession.transaction(Transaction.Type.WRITE);
                Concept firstEntity = Iterables.getOnlyElement(tx.execute(Graql.<GetQuery>parse("match $x isa entity1; get;"), false)).get("x");
        Concept secondEntity = Iterables.getOnlyElement(tx.execute(Graql.<GetQuery>parse("match $x isa entity2; get;"), false)).get("x");
        Concept resource = Iterables.getOnlyElement(tx.execute(Graql.<GetQuery>parse("match $x isa resource; get;"), false)).get("x");

        ReasonerAtomicQuery resourceQuery = ReasonerQueries.atomic(conjunction("{$x has resource $r;$r == 'inferred';$x id " + firstEntity.id().getValue() + ";}"), tx);
        String reuseResourcePatternString =
                "{" +
                        "$x has resource $r;" +
                        "$x id " + secondEntity.id().getValue() + ";" +
                        "$r id " + resource.id().getValue() + ";" +
                        "}";

        ReasonerAtomicQuery reuseResourceQuery = ReasonerQueries.atomic(conjunction(reuseResourcePatternString), tx);

        assertEquals(resourceQuery.materialise(new ConceptMap()).findFirst().orElse(null).get("r").asAttribute().isInferred(), true);

        reuseResourceQuery.materialise(new ConceptMap()).collect(Collectors.toList());
        assertEquals(Iterables.getOnlyElement(
                tx.execute(Graql.<GetQuery>parse("match" +
                                           "$x has resource $r via $rel;" +
                                           "$x id " + secondEntity.id().getValue() + ";" +
                                           "$r id " + resource.id().getValue() + ";" +
                                           "get;"), false)).get("rel").asRelation().isInferred(), true);
        assertEquals(Iterables.getOnlyElement(
                tx.execute(Graql.<GetQuery>parse("match" +
                                           "$x has resource $r via $rel;" +
                                           "$x id " + firstEntity.id().getValue() + ";" +
                                           "$r id " + resource.id().getValue() + ";" +
                                           "get;"), false)).get("rel").asRelation().isInferred(), false);
        tx.close();
    }

    @Test
    public void testWhenMaterialisingRelations_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        TransactionOLTP tx = materialisationTestSession.transaction(Transaction.Type.WRITE);
                Concept firstEntity = Iterables.getOnlyElement(tx.execute(Graql.<GetQuery>parse("match $x isa entity1; get;"), false)).get("x");
        Concept secondEntity = Iterables.getOnlyElement(tx.execute(Graql.<GetQuery>parse("match $x isa entity2; get;"), false)).get("x");

        ReasonerAtomicQuery relationQuery = ReasonerQueries.atomic(conjunction(
                "{" +
                        "$r (role1: $x, role2: $y);" +
                        "$x id " + firstEntity.id().getValue() + ";" +
                        "$y id " + secondEntity.id().getValue() + ";" +
                        "}"
                                                                   ),
                                                                   tx
        );

        assertEquals(relationQuery.materialise(new ConceptMap()).findFirst().orElse(null).get("r").asRelation().isInferred(), true);
        tx.close();
    }

    @Test
    public void testWhenCopying_TheCopyIsAlphaEquivalent() {
        TransactionOLTP tx = geoGraphSession.transaction(Transaction.Type.WRITE);
        String patternString = "{($x, $y) isa is-located-in;}";
        Conjunction<Statement> pattern = conjunction(patternString);
        ReasonerAtomicQuery atomicQuery = ReasonerQueries.atomic(pattern, tx);
        ReasonerAtomicQuery copy = ReasonerQueries.atomic(atomicQuery);
        assertEquals(atomicQuery, copy);
        assertEquals(atomicQuery.hashCode(), copy.hashCode());
        tx.close();
    }

    @Test
    public void testWhenRoleTypesAreAmbiguous_answersArePermutedCorrectly() {
        TransactionOLTP tx = geoGraphSession.transaction(Transaction.Type.WRITE);
        String childString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";
        String parentString = "match ($x, $y) isa is-located-in; get;";

                GetQuery childQuery = Graql.parse(childString);
        GetQuery parentQuery = Graql.parse(parentString);
        Set<ConceptMap> answers = tx.stream(childQuery, false).collect(toSet());
        Set<ConceptMap> fullAnswers = tx.stream(parentQuery, false).collect(toSet());
        Atom childAtom = ReasonerQueries.atomic(conjunction(childQuery.match().getPatterns()), tx).getAtom();
        Atom parentAtom = ReasonerQueries.atomic(conjunction(parentQuery.match().getPatterns()), tx).getAtom();

        MultiUnifier multiUnifier = childAtom.getMultiUnifier(childAtom, UnifierType.RULE);
        Set<ConceptMap> permutedAnswers = answers.stream()
                .flatMap(a -> multiUnifier.stream().map(a::unify))
                .collect(Collectors.toSet());

        MultiUnifier multiUnifier2 = childAtom.getMultiUnifier(parentAtom, UnifierType.RULE);
        Set<ConceptMap> permutedAnswers2 = answers.stream()
                .flatMap(a -> multiUnifier2.stream().map(a::unify))
                .collect(Collectors.toSet());

        assertEquals(fullAnswers, permutedAnswers2);
        assertEquals(answers, permutedAnswers);
        tx.close();
    }

    private Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Pattern.parse(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Pattern.and(vars);
    }

    private Conjunction<Statement> conjunction(Conjunction<Pattern> pattern) {
        Set<Statement> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Pattern.and(vars);
    }


    private Concept getConceptByResourceValue(TransactionOLTP tx, String id) {
        Set<Concept> instances = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(Collectors.toSet());
        if (instances.size() != 1) {
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        }
        return instances.iterator().next();
    }

}
