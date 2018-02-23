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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.engine;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.grpc.GrpcTestUtil;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import io.grpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * @author Felix Chapman
 */
public class GrpcServerIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.create();

    private final GraknSession localSession = engine.sessionWithNewKeyspace();

    private GraknSession remoteSession;

    @Before
    public void setUp() {
        remoteSession = RemoteGrakn.session(engine.grpcUri(), localSession.keyspace());
    }

    @After
    public void tearDown() {
        remoteSession.close();
    }

    @Test
    public void whenExecutingAndCommittingAQuery_TheQueryIsCommitted() throws InterruptedException {
        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            tx.graql().define(label("person").sub("entity")).execute();
            tx.commit();
        }

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            assertNotNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void whenExecutingAQueryAndNotCommitting_TheQueryIsNotCommitted() throws InterruptedException {
        try (GraknTx tx = remoteSession.open(GraknTxType.WRITE)) {
            tx.graql().define(label("person").sub("entity")).execute();
        }

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            assertNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void whenExecutingAQuery_ResultsAreReturned() throws InterruptedException {
        List<Answer> answers;

        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            answers = tx.graql().match(var("x").sub("thing")).get().execute();
        }

        int numMetaTypes = Schema.MetaSchema.METATYPES.size();
        assertThat(answers.toString(), answers, hasSize(numMetaTypes));
        assertThat(Sets.newHashSet(answers), hasSize(numMetaTypes));

        try (GraknTx tx = localSession.open(GraknTxType.READ)) {
            for (Answer answer : answers) {
                assertThat(answer.vars(), contains(var("x")));
                assertNotNull(tx.getConcept(answer.get("x").getId()));
            }
        }
    }

    @Test
    public void whenExecutingTwoSequentialQueries_ResultsAreTheSame() throws InterruptedException {
        Set<Answer> answers1;
        Set<Answer> answers2;

        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            answers1 = tx.graql().match(var("x").sub("thing")).get().stream().collect(Collectors.toSet());
            answers2 = tx.graql().match(var("x").sub("thing")).get().stream().collect(Collectors.toSet());
        }

        assertEquals(answers1, answers2);
    }

    @Test // This behaviour is temporary - we should eventually support it correctly
    public void whenExecutingTwoParallelQueries_Throw() throws Throwable {
        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            GetQuery query = tx.graql().match(var("x").sub("thing")).get();

            Iterator<Answer> iterator1 = query.iterator();
            Iterator<Answer> iterator2 = query.iterator();

            exception.expect(GrpcTestUtil.hasStatus(Status.FAILED_PRECONDITION));

            while (iterator1.hasNext() || iterator2.hasNext()) {
                if (iterator1.hasNext()) iterator1.next();
                if (iterator2.hasNext()) iterator2.next();
            }
        }
    }

    @Test
    public void whenExecutingAnInvalidQuery_Throw() throws Throwable {
        try (GraknTx tx = remoteSession.open(GraknTxType.READ)) {
            GetQuery query = tx.graql().match(var("x").isa("not-a-thing")).get();

            exception.expect(GraqlQueryException.class);
            exception.expectMessage(GraqlQueryException.labelNotFound(Label.of("not-a-thing")).getMessage());

            query.execute();
        }
    }
}
