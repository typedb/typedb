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
 *
 */

package grakn.core.graql.executor;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DirectSubIT {

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
    }

    @Before
    public void newTransaction() {
        tx = session.writeTransaction();
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    @Test
    public void directSubReturnsOnlyItselfAndDirectChildTypesWithoutReasoning() {
        tx.execute(Graql.parse("define person sub entity; child sub person;").asDefine());
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> directSubs = tx.execute(Graql.match(Graql.var("x").subX("entity")).get(), false);
        assertEquals(2, directSubs.size());
        assertFalse(directSubs.stream().anyMatch(conceptMap -> conceptMap.get("x").asType().label().equals(Label.of("child"))));
    }

    @Test
    public void directSubReturnsOnlyItselfAndDirectChildTypesWithReasoning() {
        tx.execute(Graql.parse("define person sub entity; child sub person;").asDefine());
        tx.commit();

        tx = session.writeTransaction();
        List<ConceptMap> directSubs = tx.execute(Graql.match(Graql.var("x").subX("entity")).get());
        assertEquals(2, directSubs.size());
        assertFalse(directSubs.stream().anyMatch(conceptMap -> conceptMap.get("x").asType().label().equals(Label.of("child"))));
    }


    // TODO when subX is in the ConceptAPI, we can add this test
    @Ignore
    @Test
    public void directSubInConceptAPIReturnsOnlyItselfAndDirectChildTypes() {
    }
}
