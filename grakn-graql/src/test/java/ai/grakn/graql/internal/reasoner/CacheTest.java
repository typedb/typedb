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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.cache.SimpleQueryCache;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.cache.TxRuleCache;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheTest {

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("ruleApplicabilityTest.gql");

    private static ReasonerAtomicQuery recordQuery;
    private static ReasonerAtomicQuery retrieveQuery;
    private static ConceptMap singleAnswer;
    private static Unifier retrieveToRecordUnifier;
    private static Unifier recordToRetrieveUnifier;

    @Before
    public void onStartup(){
        EmbeddedGraknTx<?> graph = testContext.tx();
        String recordPatternString = "{(role1: $x, role2: $y) isa reifiable-relation;}";
        String retrievePatternString = "{(role1: $p1, role2: $p2) isa reifiable-relation;}";
        Conjunction<VarPatternAdmin> recordPattern = conjunction(recordPatternString, graph);
        Conjunction<VarPatternAdmin> retrievePattern = conjunction(retrievePatternString, graph);
        recordQuery = ReasonerQueries.atomic(recordPattern, graph);
        retrieveQuery = ReasonerQueries.atomic(retrievePattern, graph);
        retrieveToRecordUnifier = retrieveQuery.getMultiUnifier(recordQuery).getUnifier();
        recordToRetrieveUnifier = retrieveToRecordUnifier.inverse();

        Entity entity = graph.getEntityType("anotherNoRoleEntity").instances().findFirst().orElse(null);
        singleAnswer = new ConceptMapImpl(
                ImmutableMap.of(
                        var("x"), entity,
                        var("y"), entity
                ));
    }

    @Test
    public void recordRetrieveAnswers(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        QueryAnswers record = cache.record(recordQuery, new QueryAnswers(recordQuery.getQuery().execute()));
        assertEquals(record, cache.getAnswers(retrieveQuery).unify(retrieveToRecordUnifier));
        assertEquals(record, cache.getAnswers(recordQuery));
    }

    @Test
    public void recordUpdateRetrieveAnswers(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        cache.record(recordQuery, new QueryAnswers(recordQuery.getQuery().execute()));
        cache.record(recordQuery, singleAnswer);
        assertTrue(cache.getAnswers(recordQuery).contains(singleAnswer));
        assertTrue(cache.getAnswers(retrieveQuery).contains(singleAnswer.unify(recordToRetrieveUnifier)));
    }

    @Test
    public void recordRetrieveAnswerStream(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        Set<ConceptMap> record = cache.record(recordQuery, recordQuery.getQuery().stream()).collect(Collectors.toSet());
        assertEquals(record, cache.getAnswerStream(retrieveQuery).map(ans -> ans.unify(retrieveToRecordUnifier)).collect(Collectors.toSet()));
        assertEquals(record, cache.record(recordQuery, recordQuery.getQuery().stream()).collect(Collectors.toSet()));
    }

    @Test
    public void recordUpdateRetrieveAnswerStream(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        cache.record(recordQuery, recordQuery.getQuery().stream());
        cache.record(recordQuery, singleAnswer);

        assertTrue(cache.getAnswerStream(recordQuery).anyMatch(ans -> ans.equals(singleAnswer)));
        assertTrue(cache.getAnswerStream(retrieveQuery).anyMatch(ans -> ans.equals(singleAnswer.unify(recordToRetrieveUnifier))));
    }

    @Test
    public void getRetrieveAnswerStream() {
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        ConceptMap answer = recordQuery.getQuery().stream().findFirst().orElse(null);
        ConceptMap retrieveAnswer = answer.unify(recordToRetrieveUnifier);

        Stream<ConceptMap> recordStream = cache.getAnswerStream(recordQuery);
        Stream<ConceptMap> retrieveStream = cache.getAnswerStream(retrieveQuery);

        QueryAnswers records = new QueryAnswers(recordStream.collect(Collectors.toSet()));
        QueryAnswers retrieveAnswers = new QueryAnswers(retrieveStream.collect(Collectors.toSet()));

        assertTrue(records.contains(answer));
        assertTrue(retrieveAnswers.contains(retrieveAnswer));
    }

    @Test
    public void getUpdateRetrieveAnswerStream() {
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        ConceptMap answer = recordQuery.getQuery().stream().findFirst().orElse(null);
        ConceptMap retrieveAnswer = answer.unify(recordToRetrieveUnifier);
        ConceptMap retrieveSingleAnswer = singleAnswer.unify(recordToRetrieveUnifier);
        Stream<ConceptMap> recordStream = cache.getAnswerStream(recordQuery);
        Stream<ConceptMap> retrieveStream = cache.getAnswerStream(retrieveQuery);

        cache.record(recordQuery, singleAnswer);

        QueryAnswers records = new QueryAnswers(recordStream.collect(Collectors.toSet()));
        QueryAnswers retrieveAnswers = new QueryAnswers(retrieveStream.collect(Collectors.toSet()));

        //NB: not expecting the update in the stream
        assertTrue(records.contains(answer));
        assertTrue(retrieveAnswers.contains(retrieveAnswer));
        assertFalse(records.contains(singleAnswer));
        assertFalse(retrieveAnswers.contains(retrieveSingleAnswer));

        assertTrue(cache.getAnswers(recordQuery).contains(singleAnswer));
        assertTrue(cache.getAnswers(retrieveQuery).contains(retrieveSingleAnswer));
    }

    @Test
    public void recordRetrieveSingleAnswer(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        ConceptMap answer = recordQuery.getQuery().stream().findFirst().orElse(null);
        ConceptMap retrieveAnswer = answer.unify(recordToRetrieveUnifier);
        cache.record(recordQuery, answer);

        assertEquals(cache.getAnswer(recordQuery, new ConceptMapImpl()), new ConceptMapImpl());
        assertEquals(cache.getAnswer(recordQuery, answer), answer);
        assertEquals(cache.getAnswer(recordQuery, retrieveAnswer), answer);

        assertEquals(cache.getAnswer(retrieveQuery, new ConceptMapImpl()), new ConceptMapImpl());
        assertEquals(cache.getAnswer(retrieveQuery, retrieveAnswer), retrieveAnswer);
        assertEquals(cache.getAnswer(retrieveQuery, answer), retrieveAnswer);
    }

    /**
     * ##################################
     *
     *      Lazy query cache tests
     *
     * ##################################
     */

    @Test
    public void lazilyRecordRetrieveAnswers(){
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        cache.record(recordQuery, recordQuery.getQuery().stream());

        Set<ConceptMap> retrieve = cache.getAnswerStream(retrieveQuery).map(ans -> ans.unify(retrieveToRecordUnifier)).collect(toSet());
        Set<ConceptMap> record = cache.getAnswerStream(recordQuery).collect(toSet());

        assertTrue(!retrieve.isEmpty());
        assertEquals(record, retrieve);
    }

    @Test
    public void lazilyRecordUpdateRetrieveAnswers(){
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        cache.record(recordQuery, recordQuery.getQuery().stream());
        cache.record(recordQuery, Stream.of(singleAnswer));

        Set<ConceptMap> retrieve = cache.getAnswerStream(retrieveQuery).map(ans -> ans.unify(retrieveToRecordUnifier)).collect(toSet());
        Set<ConceptMap> record = cache.getAnswerStream(recordQuery).collect(toSet());

        assertTrue(!retrieve.isEmpty());
        assertTrue(retrieve.contains(singleAnswer));
        assertEquals(record, retrieve);
    }

    @Test
    public void lazilyGetRetrieveAnswers() {
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        cache.record(recordQuery, recordQuery.getQuery().stream());
        LazyAnswerIterator retrieveIterator = cache.getAnswers(retrieveQuery);
        LazyAnswerIterator recordIterator = cache.getAnswers(recordQuery);

        Set<ConceptMap> record = recordIterator.stream().collect(toSet());
        Set<ConceptMap> retrieve = retrieveIterator.stream().map(ans -> ans.unify(retrieveToRecordUnifier)).collect(toSet());

        assertTrue(!retrieve.isEmpty());
        assertEquals(record, retrieve);
    }

    @Test
    public void lazilyGetUpdateRetrieveAnswers(){
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        ConceptMap retrieveSingleAnswer = singleAnswer.unify(recordToRetrieveUnifier);
        cache.record(recordQuery, recordQuery.getQuery().stream());
        LazyAnswerIterator retrieveIterator = cache.getAnswers(retrieveQuery);
        LazyAnswerIterator recordIterator = cache.getAnswers(recordQuery);

        cache.record(recordQuery, Stream.of(singleAnswer));

        Set<ConceptMap> record = recordIterator.stream().collect(toSet());
        Set<ConceptMap> retrieve = retrieveIterator.stream().map(ans -> ans.unify(retrieveToRecordUnifier)).collect(toSet());

        assertTrue(!retrieve.isEmpty());
        assertTrue(!retrieve.contains(singleAnswer));
        assertEquals(record, retrieve);

        assertTrue(cache.getAnswers(recordQuery).stream().anyMatch(ans -> ans.equals(singleAnswer)));
        assertTrue(cache.getAnswers(retrieveQuery).stream().anyMatch(ans -> ans.equals(retrieveSingleAnswer)));
    }

    /**
     * ##################################
     *
     *      Rule cache tests
     *
     * ##################################
     */

    @Test
    public void whenGettingRulesWithType_correctRulesAreObtained(){
        EmbeddedGraknTx<?> tx = testContext.tx();
        TxRuleCache ruleCache = tx.ruleCache();

        SchemaConcept reifyingRelation = tx.getSchemaConcept(Label.of("reifying-relation"));
        SchemaConcept ternary = tx.getSchemaConcept(Label.of("ternary"));
        Set<Rule> rulesWithBinary = ruleCache.getRulesWithType(reifyingRelation).collect(toSet());
        Set<Rule> rulesWithTernary = ruleCache.getRulesWithType(ternary).collect(toSet());

        assertEquals(2, rulesWithBinary.size());
        assertEquals(2, rulesWithTernary.size());

        rulesWithBinary.stream()
                .map(r -> ruleCache.getRule(r, () -> new InferenceRule(r, tx)))
                .forEach(r -> assertEquals(reifyingRelation, r.getHead().getAtom().getSchemaConcept()));
        rulesWithTernary.stream()
                .map(r -> ruleCache.getRule(r, () -> new InferenceRule(r, tx)))
                .forEach(r -> assertEquals(ternary, r.getHead().getAtom().getSchemaConcept()));
    }

    @Test
    public void whenAddingARule_cacheContainsUpdatedEntry(){
        EmbeddedGraknTx<?> tx = testContext.tx();

        Pattern when = tx.graql().parser().parsePattern("{$x isa entity;$y isa entity;}");
        Pattern then = tx.graql().parser().parsePattern("{(role1: $x, role2: $y) isa binary;}");
        Rule dummyRule = tx.putRule("dummyRule", when, then);

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));
        Set<Rule> cachedRules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
        assertTrue(cachedRules.contains(dummyRule));
    }

    @Test
    public void whenAddingARuleAfterClosingTx_cacheContainsConsistentEntry(){
        EmbeddedGraknTx<?> oldTx = testContext.tx();
        EmbeddedGraknSession session = testContext.tx().session();
        oldTx.close();
        EmbeddedGraknTx tx = session.transaction(GraknTxType.WRITE);

        Pattern when = tx.graql().parser().parsePattern("{$x isa entity;$y isa entity;}");
        Pattern then = tx.graql().parser().parsePattern("{(role1: $x, role2: $y) isa binary;}");
        Rule dummyRule = tx.putRule("dummyRule", when, then);

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));

        Set<Rule> commitedRules = binary.thenRules().collect(Collectors.toSet());
        Set<Rule> cachedRules = tx.ruleCache().getRulesWithType(binary).collect(toSet());
        assertEquals(Sets.union(commitedRules, Sets.newHashSet(dummyRule)), cachedRules);
    }

    //TODO: currently we do not acknowledge deletions
    @Ignore
    @Test
    public void whenDeletingARule_cacheContainsUpdatedEntry(){
        EmbeddedGraknTx<?> tx = testContext.tx();
        tx.graql().parse("undefine $x sub rule label 'rule-0';").execute();

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));
        Set<Rule> rules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
        assertTrue(rules.isEmpty());
    }


    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
