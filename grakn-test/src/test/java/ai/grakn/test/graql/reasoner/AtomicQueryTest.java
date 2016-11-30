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

package ai.grakn.test.graql.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.test.AbstractEngineTest;
import ai.grakn.test.graql.reasoner.graphs.AdmissionsGraph;
import ai.grakn.test.graql.reasoner.graphs.SNBGraph;
import ai.grakn.test.graql.reasoner.graphs.TestGraph;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.PatternAdmin;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtomicQueryTest extends AbstractEngineTest{
    private static GraknGraph graph;
    private static QueryBuilder qb;

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() {
        assumeTrue(usingTinker());
        graph = SNBGraph.getGraph();
        qb = graph.graql();
    }

    @Test
    public void testErrorNonAtomicQuery() {
        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation;($y, $t) isa typing;";
        exception.expect(IllegalStateException.class);
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
    }

    @Test
    public void testCopyConstructor(){
        String queryString = "match ($x, $y) isa recommendation;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
        AtomicQuery copy = new AtomicQuery(atomicQuery);
        assert(atomicQuery.equals(copy));
        assert(atomicQuery.hashCode() == copy.hashCode());
    }

    @Test
    public void testPatternNotVar(){
        exception.expect(IllegalArgumentException.class);
        Conjunction<PatternAdmin> pattern = qb.<MatchQuery>parse("match $x isa person;").admin().getPattern();
        exception.expectMessage(ErrorMessage.PATTERN_NOT_VAR.getMessage(pattern.toString()));
        Atomic atom = AtomicFactory.create(pattern);
    }

    @Test
    public void testMaterialize(){
        assert(!qb.<AskQuery>parse("match ($x, $y) isa recommendation;$x has name 'Bob';$y has name 'Colour of Magic'; ask;").execute());

        String queryString = "match ($x, $y) isa recommendation;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
        atomicQuery.materialise(Sets.newHashSet(new IdPredicate("x", getConcept("Bob"))
                                                , new IdPredicate("y", getConcept("Colour of Magic"))));
        assert(qb.<AskQuery>parse("match ($x, $y) isa recommendation;$x has name 'Bob';$y has name 'Colour of Magic'; ask;").execute());
    }

    //TODO Bug #10655 if we group resources into atomic queries
    @Test
    @Ignore
    public void testUnification(){
        GraknGraph localGraph = TestGraph.getGraph("name", "ancestor-friend-test.gql");
        AtomicQuery parentQuery = new AtomicQuery("match ($Y, $z) isa Friend; $Y has name 'd'; select $z;", localGraph);
        AtomicQuery childQuery = new AtomicQuery("match ($X, $Y) isa Friend; $Y has name 'd'; select $X;", localGraph);

        Atomic parentAtom = parentQuery.getAtom();
        Atomic childAtom = childQuery.getAtom();
        Map<String, String> unifiers = childAtom.getUnifiers(parentAtom);
        Map<String, String> correctUnifiers = new HashMap<>();
        correctUnifiers.put("X", "z");
        assertTrue(unifiers.equals(correctUnifiers));
    }

    @Test
    public void testResourceEquivalence(){
        String queryString = "match" + "" +
                "$x-firstname-9cbf242b-6baf-43b0-97a3-f3af5d801777 value 'c';" +
                "$x has firstname $x-firstname-9cbf242b-6baf-43b0-97a3-f3af5d801777;";
        String queryString2 = "match" +
                "$x has firstname $x-firstname-d6a3b1d0-2a1c-48f3-b02e-9a6796e2b581;" +
                "$x-firstname-d6a3b1d0-2a1c-48f3-b02e-9a6796e2b581 value 'c';";
        AtomicQuery parentQuery = new AtomicQuery(queryString, graph);
        AtomicQuery childQuery = new AtomicQuery(queryString2, graph);
        assertTrue(parentQuery.equals(childQuery));
        assertTrue(parentQuery.hashCode() == childQuery.hashCode());
    }

    @Test
    public void testResourceEquivalence2() {
        GraknGraph lgraph = AdmissionsGraph.getGraph();
        String queryString = "match $x isa $x-type-ec47c2f8-4ced-46a6-a74d-0fb84233e680;" +
                "$x has GRE $x-GRE-dabaf2cf-b797-4fda-87b2-f9b01e982f45;" +
                "$x-type-ec47c2f8-4ced-46a6-a74d-0fb84233e680 type-name 'applicant';" +
                "$x-GRE-dabaf2cf-b797-4fda-87b2-f9b01e982f45 value > 1099;";

        String queryString2 = "match $x isa $x-type-79e3295d-6be6-4b15-b691-69cf634c9cd6;" +
                "$x has GRE $x-GRE-388fa981-faa8-4705-984e-f14b072eb688;" +
                "$x-type-79e3295d-6be6-4b15-b691-69cf634c9cd6 type-name 'applicant';" +
                "$x-GRE-388fa981-faa8-4705-984e-f14b072eb688 value > 1099;";
        AtomicQuery parentQuery = new AtomicQuery(queryString, lgraph);
        AtomicQuery childQuery = new AtomicQuery(queryString2, lgraph);
        assertTrue(parentQuery.equals(childQuery));
        assertTrue(parentQuery.hashCode() == childQuery.hashCode());
    }

    private static Concept getConcept(String id){
        Set<Concept> instances = graph.getResourcesByValue(id)
                .stream().flatMap(res -> res.ownerInstances().stream()).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

}
