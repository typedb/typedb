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

package grakn.core.graql.internal.gremlin.fragment;

import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.Match;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.graql.query.Var;
import grakn.core.graql.query.QueryBuilderImpl;
import org.junit.Test;

import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;

public class IsaExplicitTest {

    private static final Var x = var("x");
    private static final Var y = var("y");

    private static final String thingy = "thingy";
    private static final String thingy1 = "thingy1";

    @Test
    public void testInsertSyntax() {
        QueryBuilder queryBuilder = new QueryBuilderImpl();
        InsertQuery insertQuery;

        insertQuery = queryBuilder.insert(x.isaExplicit(thingy));
        assertEquals("insert $x isa! thingy;", insertQuery.toString());

        insertQuery = queryBuilder.parse("insert $x isa! thingy;");
        assertEquals(queryBuilder.insert(x.isaExplicit(thingy)), insertQuery);
    }



    @Test
    public void testMatchSyntax() {
        QueryBuilder queryBuilder = new QueryBuilderImpl();
        Match matchQuery;
        GetQuery getQuery;

        matchQuery = queryBuilder.match(x.isaExplicit(thingy1));
        assertEquals("match $x isa! thingy1;", matchQuery.toString());

        matchQuery = queryBuilder.match(x.isaExplicit(y));
        assertEquals("match $x isa! $y;", matchQuery.toString());

        getQuery = queryBuilder.parse("match $x isa! thingy1; get;");
        assertEquals(queryBuilder.match(x.isaExplicit(thingy1)), getQuery.match());

        getQuery = queryBuilder.parse("match $x isa! $y; get;");
        assertEquals(queryBuilder.match(x.isaExplicit(y)), getQuery.match());
    }
}