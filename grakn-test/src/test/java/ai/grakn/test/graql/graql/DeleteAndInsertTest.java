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

package ai.grakn.test.graql.graql;

import ai.grakn.GraknGraph;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.test.GraphContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.NO_PATTERNS;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;

public class DeleteAndInsertTest {

    private GraknGraph graph = rule.graph();

    @ClassRule
    public static final GraphContext rule = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
    }

    @Test
    public void whenDeletingAVariableNotInTheQuery_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(VarName.of("y")));
        graph.graql().match(var("x").isa("movie")).delete("y").execute();
    }

    @Test
    public void whenDeletingAnEmptyPattern_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        graph.graql().match(var()).delete(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void deleteVarNameNullSet() {
        graph.graql().match(var()).delete((Set<Var>) null).execute();
    }

    @Test(expected = Exception.class)
    public void whenDeleteIsPassedNull_Throw() {
        graph.graql().match(var()).delete((String) null).execute();
    }

    @Test(expected = Exception.class)
    public void matchInsertNullVar() {
        graph.graql().match(var("x").isa("movie")).insert((Var) null).execute();
    }

    @Test(expected = Exception.class)
    public void matchInsertNullCollection() {
        graph.graql().match(var("x").isa("movie")).insert((Collection<? extends Var>) null).execute();
    }

    @Test
    public void whenMatchInsertingAnEmptyPattern_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        graph.graql().match(var()).insert(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullVar() {
        graph.graql().insert((Var) null).execute();
    }

    @Test(expected = Exception.class)
    public void insertNullCollection() {
        graph.graql().insert((Collection<? extends Var>) null).execute();
    }

    @Test
    public void whenInsertingAnEmptyPattern_Throw() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        graph.graql().insert(Collections.EMPTY_SET).execute();
    }
}