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

package grakn.core.graql.reasoner.cache;

import com.google.common.collect.ImmutableMap;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.explanation.LookupExplanation;
import grakn.core.graql.reasoner.explanation.RuleExplanation;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("CheckReturnValue")
public class QueryCacheIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    @BeforeClass
    public static void loadContext() {
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);
    }


    @AfterClass
    public static void closeSession() {
        genericSchemaSession.close();
    }

    @Test
    public void whenRecordingAndMatchExists_entryIsUpdated(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction("(role: $x, role: $y) isa binary;", tx), tx);

            //mock answer
            ConceptMap specificAnswer = new ConceptMap(ImmutableMap.of(
                    Graql.var("x").var(), tx.getEntityType("entity").instances().iterator().next(),
                    Graql.var("y").var(), tx.getEntityType("entity").instances().iterator().next()),
                    new LookupExplanation(query.getPattern())
            );

            //record parent
            tx.execute(query.getQuery()).stream()
                    .map(ans -> ans.explain(new LookupExplanation(query.getPattern())))
                    .filter(ans -> !ans.equals(specificAnswer))
                    .forEach(ans -> cache.record(query, ans));

            CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> cacheEntry = cache.getEntry(query);
            assertFalse(cacheEntry.cachedElement().contains(specificAnswer));
            cache.record(query, specificAnswer);
            assertTrue(cacheEntry.cachedElement().contains(specificAnswer));
        }
    }

    @Test
    public void whenRecordingAndMatchDoesntExist_answersArePropagatedFromParents(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            Concept mConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 'm';get;")).iterator().next().get("x");
            Concept sConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 's';get;")).iterator().next().get("x");

            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(
                    "{" +
                            "(subRole1: $x, subRole2: $y) isa binary;" +
                            "$x id '" + mConcept.id().getValue() + "';" +
                            "$y id '" + sConcept.id().getValue() + "';" +
                            "};"
                    , tx), tx);

            //mock a specific answer
            ConceptMap specificAnswer = new ConceptMap(ImmutableMap.of(
                    Graql.var("x").var(), mConcept,
                    Graql.var("y").var(), sConcept),
                    new LookupExplanation(childQuery.getPattern())
            );

            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("(role: $x, role: $y) isa binary;", tx), tx);

            //record parent
            tx.execute(parentQuery.getQuery()).stream()
                    .map(ans -> ans.explain(new LookupExplanation(parentQuery.getPattern())))
                    .filter(ans -> !ans.equals(specificAnswer))
                    .forEach(ans -> cache.record(parentQuery, ans));

            assertNull(cache.getEntry(childQuery));
            cache.record(childQuery, specificAnswer);
            CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> cacheEntry = cache.getEntry(childQuery);
            assertEquals(tx.stream(Graql.<GraqlGet>parse("match (subRole1: $x, subRole2: $y) isa binary; get;")).collect(toSet()), cacheEntry.cachedElement().getAll());
        }
    }

    @Test
    public void whenGettingAndMatchDoesntExist_answersFetchedFromDB(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("(subRole1: $x, subRole2: $y) isa binary;", tx), tx);
            Set<ConceptMap> cacheAnswers = cache.getAnswers(childQuery);
            assertEquals(tx.stream(childQuery.getQuery()).collect(toSet()), cacheAnswers);

            assertNull(cache.getEntry(childQuery));
        }
    }

    @Test
    public void whenGettingAndMatchDoesntExist_parentAvailable_answersFetchedFromParents(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("(role: $x, role: $y) isa binary;", tx), tx);
            //record parent
            tx.execute(parentQuery.getQuery()).stream()
                    .map(ans -> ans.explain(new LookupExplanation(parentQuery.getPattern())))
                    .forEach(ans -> cache.record(parentQuery, ans));
            cache.ackDBCompleteness(parentQuery);

            //retrieve child
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("(subRole1: $x, subRole2: $y) isa binary;", tx), tx);
            Set<ConceptMap> cacheAnswers = cache.getAnswers(childQuery);
            assertEquals(tx.stream(childQuery.getQuery()).collect(toSet()), cacheAnswers);

            assertEquals(cacheAnswers, cache.getEntry(childQuery).cachedElement().getAll());
        }
    }

    @Test
    public void whenGettingAndMatchExists_queryNotGround_queryDBComplete_answersFetchedFromCache(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction("(subRole1: $x, subRole2: $y) isa binary;", tx), tx);
            //record
            tx.execute(query.getQuery()).stream()
                    .map(ans -> ans.explain(new LookupExplanation(query.getPattern())))
                    .forEach(ans -> cache.record(query, ans));
            cache.ackDBCompleteness(query);

            //retrieve
            Set<ConceptMap> cacheAnswers = cache.getAnswers(query);
            assertEquals(tx.stream(query.getQuery()).collect(toSet()), cacheAnswers);

            assertEquals(cacheAnswers, cache.getEntry(query).cachedElement().getAll());
        }
    }

    @Test
    public void whenGettingAndMatchExists_queryNotGround_queryNotDBComplete_answersFetchedFromDBAndCache(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction("(subRole1: $x, subRole2: $y) isa binary;", tx), tx);
            //record
            tx.execute(query.getQuery()).stream()
                    .map(ans -> ans.explain(new LookupExplanation(query.getPattern())))
                    .forEach(ans -> cache.record(query, ans));
            cache.ackDBCompleteness(query);

            //mock a rule explained answer
            ConceptMap inferredAnswer = new ConceptMap(ImmutableMap.of(
                    Graql.var("x").var(), tx.getEntityType("entity").instances().iterator().next(),
                    Graql.var("y").var(), tx.getEntityType("entity").instances().iterator().next()),
                    new RuleExplanation(query.getPattern(), tx.getMetaRule().id())
            );
            cache.record(query, inferredAnswer);

            //retrieve
            Set<ConceptMap> cacheAnswers = cache.getAnswers(query);
            assertTrue(cacheAnswers.containsAll(tx.stream(query.getQuery()).collect(toSet())));
            assertTrue(cacheAnswers.contains(inferredAnswer));
            assertEquals(cacheAnswers, cache.getEntry(query).cachedElement().getAll());
        }
    }

    @Test
    public void whenGettingAndMatchExists_queryGround_answerFound_answersFetchedFromCache(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("(role: $x, role: $y) isa binary;", tx), tx);
            //record parent
            tx.stream(parentQuery.getQuery())
                    .map(ans -> ans.explain(new LookupExplanation(parentQuery.getPattern())))
                    .forEach(ans -> cache.record(parentQuery, ans));
            cache.ackDBCompleteness(parentQuery);

            Concept mConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 'm';get;")).iterator().next().get("x");
            Concept sConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 's';get;")).iterator().next().get("x");
            Concept fConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 'f';get;")).iterator().next().get("x");

            //record child
            ReasonerAtomicQuery preChildQuery = ReasonerQueries.atomic(conjunction(
                    "{" +
                            "(subRole1: $x, subRole2: $y) isa binary;" +
                            "$x id '" + fConcept.id().getValue() + "';" +
                            "$y id '" + sConcept.id().getValue() + "';" +
                            "};"
                    , tx), tx);
            tx.stream(preChildQuery.getQuery())
                    .map(ans -> ans.explain(new LookupExplanation(preChildQuery.getPattern())))
                    .forEach(ans -> cache.record(preChildQuery, ans));

            //retrieve child
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(
                    "{" +
                    "(subRole1: $x, subRole2: $y) isa binary;" +
                                "$x id '" + mConcept.id().getValue() + "';" +
                                "$y id '" + sConcept.id().getValue() + "';" +
                            "};"
                    , tx), tx);

            //mock a specific answer
            ConceptMap specificAnswer = new ConceptMap(ImmutableMap.of(
                    Graql.var("x").var(), mConcept,
                    Graql.var("y").var(), sConcept),
                    new LookupExplanation(childQuery.getPattern())
            );
            Set<ConceptMap> cacheAnswers = cache.getAnswers(childQuery);
            assertEquals(tx.stream(childQuery.getQuery()).collect(toSet()), cacheAnswers);
            assertTrue(cacheAnswers.contains(specificAnswer));
        }
    }

    @Test
    public void whenGettingAndMatchExists_queryGround_queryNotDBComplete_answerNotFound_answersFetchedFromDbAndCache(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().read()) {
            MultilevelSemanticCache cache = new MultilevelSemanticCache();

            Concept mConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 'm';get;")).iterator().next().get("x");
            Concept sConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 's';get;")).iterator().next().get("x");
            Concept fConcept = tx.stream(Graql.<GraqlGet>parse("match $x has resource 'f';get;")).iterator().next().get("x");

            //retrieve child
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction(
                    "{" +
                            "(subRole1: $x, subRole2: $y) isa binary;" +
                            "$x id '" + mConcept.id().getValue() + "';" +
                            "};"
                    , tx), tx);

            //mock a specific answer
            ConceptMap specificAnswer = new ConceptMap(ImmutableMap.of(
                    Graql.var("x").var(), mConcept,
                    Graql.var("y").var(), sConcept),
                    new LookupExplanation(childQuery.getPattern())
            );
            //mock a rule explained answer
            ConceptMap inferredAnswer = new ConceptMap(ImmutableMap.of(
                    Graql.var("x").var(), mConcept,
                    Graql.var("y").var(), tx.getEntityType("entity").instances().iterator().next()),
                    new RuleExplanation(childQuery.getPattern(), tx.getMetaRule().id())
            );

            //record child, we record child first so that answers do not get propagated
            ReasonerAtomicQuery preChildQuery = ReasonerQueries.atomic(conjunction(
                    "{" +
                            "(subRole1: $x, subRole2: $y) isa binary;" +
                            "$x id '" + fConcept.id().getValue() + "';" +
                            "};"
                    , tx), tx);
            tx.stream(preChildQuery.getQuery())
                    .map(ans -> ans.explain(new LookupExplanation(preChildQuery.getPattern())))
                    .forEach(ans -> cache.record(preChildQuery, ans));

            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("(role: $x, role: $y) isa binary;", tx), tx);
            //record parent
            tx.stream(parentQuery.getQuery())
                    .map(ans -> ans.explain(new LookupExplanation(parentQuery.getPattern())))
                    .filter(ans -> !ans.equals(specificAnswer))
                    .filter(ans -> !ans.equals(inferredAnswer))
                    .forEach(ans -> cache.record(parentQuery, ans));
            cache.ackDBCompleteness(parentQuery);

            cache.record(childQuery, inferredAnswer);

            Set<ConceptMap> cacheAnswers = cache.getAnswers(childQuery);
            assertEquals(
                    Stream.concat(
                            tx.stream(childQuery.getQuery()),
                            Stream.of(inferredAnswer)
                    ).collect(toSet()),
                    cacheAnswers);
            assertTrue(cacheAnswers.contains(specificAnswer));
        }
    }

    private Conjunction<Statement> conjunction(String patternString, TransactionOLTP tx) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
