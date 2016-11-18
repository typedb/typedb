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

package ai.grakn.test.graql.printer;

import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.AbstractMovieGraphTest;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class GraqlPrinterTest extends AbstractMovieGraphTest {

    private Printer printer;

    @Before
    public void setUp() {
        printer = Printers.graql();
    }

    @Test
    public void testRelationOutput() {
        MatchQuery query = graph.graql().match(var("r").isa("has-cast")
                .rel(var().has("name", "Al Pacino"))
                .rel(var().has("name", "Michael Corleone"))
                .rel(var().has("title", "Godfather")));

        String relationString = printer.graqlString(query.get("r").iterator().next());

        assertThat(relationString, containsString("has-cast"));
        assertThat(relationString, containsString("actor"));
        assertThat(relationString, containsString("production-with-cast"));
        assertThat(relationString, containsString("character-being-played"));
    }
}
