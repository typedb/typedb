/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.planning.gremlin.fragmenttest;

import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import org.junit.Test;

import static graql.lang.Graql.var;
import static org.junit.Assert.assertEquals;

public class IsaExplicitTest {

    private static final Statement x = var("x");
    private static final Statement y = var("y");

    private static final String thingy = "thingy";
    private static final String thingy1 = "thingy1";

    @Test
    public void testInsertSyntax() {
        GraqlInsert insertQuery;

        insertQuery = Graql.insert(x.isaX(thingy));
        assertEquals("insert $x isa! thingy;", insertQuery.toString());

        insertQuery = Graql.parse("insert $x isa! thingy;").asInsert();
        assertEquals(Graql.insert(x.isaX(thingy)), insertQuery);
    }


    @Test
    public void testMatchSyntax() {
        MatchClause matchQuery;
        GraqlGet getQuery;

        matchQuery = Graql.match(x.isaX(thingy1));
        assertEquals("match $x isa! thingy1;", matchQuery.toString());

        matchQuery = Graql.match(x.isaX(y));
        assertEquals("match $x isa! $y;", matchQuery.toString());

        getQuery = Graql.parse("match $x isa! thingy1; get;").asGet();
        assertEquals(Graql.match(x.isaX(thingy1)), getQuery.match());

        getQuery = Graql.parse("match $x isa! $y; get;").asGet();
        assertEquals(Graql.match(x.isaX(y)), getQuery.match());
    }
}