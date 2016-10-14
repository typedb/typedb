/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.graql.reasoner;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.AtomicFactory;
import io.mindmaps.graql.internal.reasoner.predicate.Substitution;
import io.mindmaps.test.graql.reasoner.graphs.SNBGraph;
import io.mindmaps.util.ErrorMessage;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AtomicQueryTest {
    private static MindmapsGraph graph;
    private static QueryBuilder qb;

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() {

        graph = SNBGraph.getGraph();
        qb = Graql.withGraph(graph);
    }

    @Test
    public void testErrorNonAtomicQuery() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(ErrorMessage.NON_ATOMIC_QUERY.getMessage());

        String queryString = "match $x isa person;$y isa product;($x, $y) isa recommendation;($y, $t) isa typing;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
    }

    @Test
    public void testCopyConstructor(){
        String queryString = "match ($x, $y) isa recommendation;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);

        assert(atomicQuery.equals(new AtomicQuery(atomicQuery)));
    }

    @Test
    public void testErrorNoParent(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(ErrorMessage.PARENT_MISSING.getMessage());

        Atomic atom = AtomicFactory.create(qb.parseMatch("match $x isa person").admin().getPattern());
        AtomicQuery query = new AtomicQuery(atom);
    }

    @Test
    public void testErrorOnMaterialize(){
        exception.expect(IllegalStateException.class);
        exception.expectMessage(ErrorMessage.MATERIALIZATION_ERROR.getMessage());

        String queryString = "match ($x, $y) isa recommendation;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
        atomicQuery.materialize(Sets.newHashSet(new Substitution("x", graph.getConcept("Bob"))));
    }

    @Test
    public void testMaterialize(){

        assert(!qb.parseAsk("match ($x, $y) isa recommendation;$x id 'Bob';$y id 'Colour of Magic'; ask;").execute());

        String queryString = "match ($x, $y) isa recommendation;";
        AtomicQuery atomicQuery = new AtomicQuery(queryString, graph);
        atomicQuery.materialize(Sets.newHashSet(new Substitution("x", graph.getConcept("Bob"))
                                                , new Substitution("y", graph.getConcept("Colour of Magic"))));

        assert(qb.parseAsk("match ($x, $y) isa recommendation;$x id 'Bob';$y id 'Colour of Magic'; ask;").execute());
    }

}
