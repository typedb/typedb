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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.query.QueryAnswerStream;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleUtil;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.GeoKB;
import ai.grakn.test.kbs.MatrixKBII;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.join;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class LazyTest {

    @ClassRule
    public static final SampleKBContext geoKB = SampleKBContext.preLoad(GeoKB.get()).assumeTrue(GraknTestSetup.usingTinker());

    @ClassRule
    public static final SampleKBContext sampleKB = SampleKBContext.empty().assumeTrue(GraknTestSetup.usingTinker());

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestSetup.usingTinker());
    }

    @Test
    public void testLazyCache(){
        GraknTx graph = geoKB.tx();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);

        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Stream<Answer> dbStream = query.getQuery().stream();
        cache.record(query, dbStream);

        Set<Answer> collect2 = cache.getAnswerStream(query2).collect(toSet());
        Set<Answer> collect = cache.getAnswerStream(query).collect(toSet());

        assertEquals(collect.size(), collect2.size());
    }

    @Test
    public void testLazyCache2(){
        GraknTx graph = geoKB.tx();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";
        String patternString3 = "{(geo-entity: $x, entity-location: $z) isa is-located-in;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(pattern3, graph);

        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Stream<Answer> stream = cache.getAnswerStream(query);
        Stream<Answer> stream2 = cache.getAnswerStream(query2);
        Stream<Answer> joinedStream = QueryAnswerStream.join(stream, stream2);

        joinedStream = cache.record(query3, joinedStream.map(a -> a.project(query3.getVarNames())));

        Set<Answer> collect = joinedStream.collect(toSet());
        Set<Answer> collect2 = cache.getAnswerStream(query3).collect(toSet());

        assertEquals(collect.size(), 37);
        assertEquals(collect.size(), collect2.size());
    }

    @Test
    public void testJoin(){
        GraknTx graph = geoKB.tx();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";
        String patternString3 = "{(geo-entity: $z, entity-location: $w) isa is-located-in;}";

        Conjunction<VarPatternAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarPatternAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarPatternAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, graph);
        ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, graph);
        ReasonerAtomicQuery query3 = ReasonerQueries.atomic(pattern3, graph);

        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        cache.getAnswerStream(query);
        InferenceRule rule = new InferenceRule(RuleUtil.getRules(graph).iterator().next(), graph);

        Set<Var> joinVars = Sets.intersection(query.getVarNames(), query2.getVarNames());
        Stream<Answer> join = join(
                query.getQuery().stream(),
                query2.getQuery().stream(),
                ImmutableSet.copyOf(joinVars)
                )
                .map(a -> a.project(rule.getHead().getVarNames()))
                .distinct()
                .map(ans -> ans.explain(new RuleExplanation(query, rule)));

        cache.record(rule.getHead(), join);

        Stream<Answer> stream = cache.getAnswerStream(rule.getHead());
        Stream<Answer> stream2 = cache.getAnswerStream(query3);
        joinVars = Sets.intersection(rule.getHead().getVarNames(), query3.getVarNames());
        List<Answer> collect = QueryAnswerStream.join(
                stream,
                stream2,
                ImmutableSet.copyOf(joinVars))
                .collect(Collectors.toList());
        assertEquals(collect.size(), 40);
    }

    @Test
    public void testKnownFilter(){
        GraknTx graph = geoKB.tx();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";
        GetQuery query = graph.graql().parse(queryString);
        QueryAnswers answers = queryAnswers(query);
        long count = query
                .stream()
                .filter(a -> QueryAnswerStream.knownFilter(a, answers.stream()))
                .count();
        assertEquals(count, 0);
    }

    @Test
    public void testLazy()  {
        final int N = 20;

        long startTime = System.currentTimeMillis();
        sampleKB.tx().close();
        sampleKB.load(MatrixKBII.get(N, N));
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("loadTime: " + loadTime);
        GraknTx graph = sampleKB.tx();

        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        String queryString = "match (P-from: $x, P-to: $y) isa P; get;";
        GetQuery query = iqb.parse(queryString);

        final int limit = 10;
        final long maxTime = 2000;
        startTime = System.currentTimeMillis();
        List<Answer> results = query.match().limit(limit).get().execute();
        long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);
        assertEquals(results.size(), limit);
        assertTrue(answerTime < maxTime);
    }

    private QueryAnswers queryAnswers(GetQuery query) {
        return new QueryAnswers(query.stream().collect(toSet()));
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
