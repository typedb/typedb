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

package grakn.core.test.common;

import grakn.client.GraknClient;
import grakn.client.GraknClient.Session;
import grakn.client.answer.ConceptMap;
import grakn.client.answer.Numeric;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraknApplicationTest {
    private GraknClient graknClient;

    @Before
    public void before() {
        String host = "localhost:48555";
        graknClient = new GraknClient(host);
    }

    @After
    public void after() {
        graknClient.close();
    }

    @Test
    public void testDeployment() {
        try (Session session = graknClient.session("grakn")) {
            try (GraknClient.Transaction tx = session.transaction().write()) {
                List<ConceptMap> result = tx.execute(Graql.match(Graql.var("t").sub("thing")).get());
                assertTrue(result.size() > 0);
            }
        }
    }

    @Test
    public void testComputeQuery() {
        try (Session session = graknClient.session("computetest")) {
            GraknClient.Transaction tx = session.transaction().write();
            tx.execute(Graql.parse("define person sub entity;").asDefine());
            tx.execute(Graql.parse("insert $x isa person;").asInsert());
            tx.commit();
            tx = session.transaction().write();
            List<Numeric> countResult = tx.execute(Graql.parse("compute count;").asComputeStatistics());
            assertEquals(1, countResult.get(0).number().intValue());
        }
    }
}