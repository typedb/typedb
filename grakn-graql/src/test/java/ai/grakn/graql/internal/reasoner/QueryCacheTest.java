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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Entity;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import com.google.common.collect.ImmutableMap;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class QueryCacheTest {

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("ruleApplicabilityTest.gql");

    private static GraknTx graph;
    private static ReasonerAtomicQuery recordQuery;
    private static ReasonerAtomicQuery retrieveQuery;
    private static Answer singleAnswer;
    private static Unifier retrieveToRecordUnifier;
    private static Unifier recordToRetrieveUnifier;

    @Before
    public void onStartup(){
        assumeTrue(GraknTestUtil.usingTinker());
        graph = testContext.tx();
        String recordPatternString = "{(role1: $x, role2: $y) isa reifiable-relation;}";
        String retrievePatternString = "{(role1: $p1, role2: $p2) isa reifiable-relation;}";
        Conjunction<VarPatternAdmin> recordPattern = conjunction(recordPatternString, graph);
        Conjunction<VarPatternAdmin> retrievePattern = conjunction(retrievePatternString, graph);
        recordQuery = ReasonerQueries.atomic(recordPattern, graph);
        retrieveQuery = ReasonerQueries.atomic(retrievePattern, graph);
        retrieveToRecordUnifier = retrieveQuery.getMultiUnifier(recordQuery).getUnifier();
        recordToRetrieveUnifier = retrieveToRecordUnifier.inverse();

        Entity entity = graph.getEntityType("anotherNoRoleEntity").instances().findFirst().orElse(null);
        singleAnswer = new QueryAnswer(
                ImmutableMap.of(
                        var("x"), entity,
                        var("y"), entity
                ));
    }

    @Test
    public void batched_Record_Retrieve(){
        QueryCache<ReasonerAtomicQuery> cache = new QueryCache<>();
        QueryAnswers record = cache.record(recordQuery, new QueryAnswers(recordQuery.getQuery().execute()));
        assertEquals(record, cache.getAnswers(retrieveQuery).unify(retrieveToRecordUnifier));
        assertEquals(record, cache.getAnswers(recordQuery));
    }

    @Test
    public void batched_Record_SingleAnswerUpdate_Retrieve(){
        QueryCache<ReasonerAtomicQuery> cache = new QueryCache<>();
        cache.record(recordQuery, new QueryAnswers(recordQuery.getQuery().execute()));
        cache.recordAnswer(recordQuery, singleAnswer);
        assertTrue(cache.getAnswers(recordQuery).contains(singleAnswer));
        assertTrue(cache.getAnswers(retrieveQuery).contains(singleAnswer.unify(recordToRetrieveUnifier)));
    }

    @Test
    public void stream_Record_Retrieve(){
        QueryCache<ReasonerAtomicQuery> cache = new QueryCache<>();
        Set<Answer> record = cache.record(recordQuery, recordQuery.getQuery().stream()).collect(Collectors.toSet());
        assertEquals(record, cache.getAnswerStream(retrieveQuery).map(ans -> ans.unify(retrieveToRecordUnifier)).collect(Collectors.toSet()));
        assertEquals(record, cache.record(recordQuery, recordQuery.getQuery().stream()).collect(Collectors.toSet()));
    }

    @Test
    public void stream_Record_SingleAnswerUpdate_Retrieve(){
        QueryCache<ReasonerAtomicQuery> cache = new QueryCache<>();
        cache.record(recordQuery, recordQuery.getQuery().stream());
        cache.recordAnswer(recordQuery, singleAnswer);

        assertTrue(cache.getAnswerStream(recordQuery).anyMatch(ans -> ans.equals(singleAnswer)));
        assertTrue(cache.getAnswerStream(retrieveQuery).anyMatch(ans -> ans.equals(singleAnswer.unify(recordToRetrieveUnifier))));
    }

    @Test
    public void singleAnswer_Record_Retrieve(){
        //TODO
    }

    @Test
    public void singeAnswer_dualRecord_Retrieve() {
        //TODO
    }

    @Test
    public void singleAnswer_Record_Update_Retrieve(){
        //TODO
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
