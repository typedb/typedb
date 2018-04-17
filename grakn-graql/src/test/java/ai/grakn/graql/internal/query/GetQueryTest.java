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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.matcher.GraknMatchers.variable;
import static ai.grakn.matcher.MovieMatchers.alPacino;
import static ai.grakn.matcher.MovieMatchers.marlonBrando;
import static ai.grakn.matcher.MovieMatchers.martinSheen;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Felix Chapman
 */
public class GetQueryTest {

    private static final Var x = var("x");
    private static final Var y = var("y");
    private static final Var z = var("z");

    private QueryBuilder qb;

    @ClassRule
    public static final SampleKBContext movieKB = MovieKB.context();

    @Before
    public void setUp() {
        qb = movieKB.tx().graql();
    }

    @Test
    public void whenGettingResultsString_StringHasExpectedContents() {
        GetQuery query = qb.match(x.isa("movie")).get();
        List<String> resultsString = query.resultsString(Printers.graql(false)).collect(toList());
        assertThat(resultsString, everyItem(allOf(containsString("$x"), containsString("movie"), containsString(";"))));
    }

    @Test
    public void whenRunningExecute_ResultIsSameAsParallelStreamingToAList() {
        GetQuery query = qb.match(x.isa("movie")).get();
        List<Answer> list = query.execute();
        assertEquals(list, query.parallelStream().collect(toList()));
    }

    @Test
    public void whenProjectingVariables_DuplicateResultsDisappear() {
        GetQuery query = qb.match(
                x.isa("person"),
                var().rel(x).rel(y),
                y.isa("movie"),
                var().rel(y).rel(z),
                z.isa("person").has("name", "Marlon Brando")
        ).get(x, z);

        assertThat(query, variable(x, containsInAnyOrder(marlonBrando, alPacino, martinSheen)));
    }
}
