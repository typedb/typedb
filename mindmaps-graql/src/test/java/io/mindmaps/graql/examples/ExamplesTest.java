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

package io.mindmaps.graql.examples;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.InsertQuery;
import io.mindmaps.graql.api.query.MatchQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ExamplesTest {

    private QueryParser qp;

    @Before
    public void setUp() {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        qp = QueryParser.create(graph.newTransaction());
    }

    @Test
    public void testPhilosophers() {
        load(
                "insert person isa entity-type;",
                "insert id 'Socrates' isa person;",
                "insert id 'Plato' isa person;",
                "insert id 'Aristotle' isa person;",
                "insert id 'Alexander' isa person;"
        );

        assertEquals(4, qp.parseMatchQuery("match $p isa person").getMatchQuery().stream().count());

        load(
                "insert school isa entity-type;",
                "insert id 'Peripateticism' isa school;",
                "insert id 'Platonism' isa school;",
                "insert id 'Idealism' isa school;",
                "insert id 'Cynicism' isa school;"
        );

        assertEquals(1, qp.parseMatchQuery("match $x id 'Cynicism'").getMatchQuery().stream().count());

        load(
                "insert practice isa relation-type;",
                "insert philosopher isa role-type;",
                "insert philosophy isa role-type;",
                "insert practice has-role philosopher, has-role philosophy;",
                "insert person plays-role philosopher;",
                "insert school plays-role philosophy;",
                "insert (philosopher Socrates, philosophy Platonism) isa practice;",
                "insert (philosopher Plato, philosophy Idealism) isa practice;",
                "insert (philosopher Plato, philosophy Platonism) isa practice;",
                "insert (philosopher Aristotle, philosophy Peripateticism) isa practice;"
        );

        assertEquals(
                2,
                qp.parseMatchQuery("match (philosopher $x, Platonism) isa practice;").getMatchQuery().stream().count()
        );

        load(
                "insert education isa relation-type;",
                "insert teacher isa role-type;",
                "insert student isa role-type;",
                "insert education has-role teacher, has-role student;",
                "insert person plays-role teacher, plays-role student;",
                "insert (teacher Socrates, student Plato) isa education;",
                "insert (teacher Plato, student Aristotle) isa education;",
                "insert (teacher Aristotle, student Alexander) isa education;"
        );

        load(
                "insert title isa resource-type, datatype string",
                "insert epithet isa resource-type, datatype string",
                "insert person has-resource title",
                "insert person has-resource epithet"
        );

        load(
                "insert Alexander has epithet 'The Great';",
                "insert Alexander has title 'Hegemon';",
                "insert Alexander has title 'King of Macedon'",
                "insert Alexander has title 'Shah of Persia'",
                "insert Alexander has title 'Pharaoh of Egypt';",
                "insert Alexander has title 'Lord of Asia';"
        );

        MatchQuery pharaoh = qp.parseMatchQuery("match $x has title contains 'Pharaoh'").getMatchQuery();
        assertEquals("Alexander", pharaoh.iterator().next().get("x").getId());

        load(
                "insert knowledge isa relation-type;",
                "insert thinker isa role-type;",
                "insert thought isa role-type;",
                "insert knowledge has-role thinker, has-role thought;",
                "insert fact isa entity-type, plays-role thought;",
                "insert person plays-role thinker",
                "insert id 'sun-fact' isa fact, value 'The Sun is bigger than the Earth';",
                "insert (thinker Aristotle, thought sun-fact) isa knowledge;",
                "insert id 'cave-fact' isa fact, value 'Caves are mostly pretty dark';",
                "insert (thinker Plato, thought cave-fact) isa knowledge;",
                "insert id 'nothing' isa fact;",
                "insert (thinker Socrates, thought nothing) isa knowledge;"
        );

        load(
                "insert knowledge plays-role thought;",
                "match $socratesKnowsNothing (Socrates, nothing) " +
                "insert (thinker Socrates, thought $socratesKnowsNothing) isa knowledge"
        );

        assertEquals(
                2,
                qp.parseMatchQuery("match (Socrates, $x) isa knowledge").getMatchQuery().stream().count()
        );
    }

    private void load(String... queries) {
        Stream.of(queries).map(qp::parseInsertQuery).forEach(InsertQuery::execute);
    }
}
