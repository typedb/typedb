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
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
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
            answers1 = tx.graql().match(var("x").sub("thing")).get().stream().collect(toSet());
            answers2 = tx.graql().match(var("x").sub("thing")).get().stream().collect(toSet());
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
    public void whenGettingAConcept_TheConceptContainsInformationAboutItself() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x")).get();

            for (Answer answer : query) {
                Concept remoteConcept = answer.get("x");
                Concept localConcept = localTx.getConcept(remoteConcept.getId());

                assertEquals(localConcept.isAttribute(), remoteConcept.isAttribute());
                assertEquals(localConcept.isAttributeType(), remoteConcept.isAttributeType());
                assertEquals(localConcept.isEntity(), remoteConcept.isEntity());
                assertEquals(localConcept.isEntityType(), remoteConcept.isEntityType());
                assertEquals(localConcept.isRelationship(), remoteConcept.isRelationship());
                assertEquals(localConcept.isRelationshipType(), remoteConcept.isRelationshipType());
                assertEquals(localConcept.isRole(), remoteConcept.isRole());
                assertEquals(localConcept.isRule(), remoteConcept.isRule());
                assertEquals(localConcept.isSchemaConcept(), remoteConcept.isSchemaConcept());
                assertEquals(localConcept.isThing(), remoteConcept.isThing());
                assertEquals(localConcept.isType(), remoteConcept.isType());
                assertEquals(localConcept.getId(), remoteConcept.getId());
                assertEquals(localConcept.isDeleted(), remoteConcept.isDeleted());
                assertEquals(localConcept.keyspace(), remoteConcept.keyspace());
            }
        }
    }

    @Ignore // TODO: implement missing methods
    @Test
    public void whenGettingASchemaConcept_TheSchemaConceptContainsInformationAboutItself() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").label("role")).get();
            SchemaConcept remoteConcept = query.stream().findAny().get().get("x").asSchemaConcept();
            SchemaConcept localConcept = localTx.getConcept(remoteConcept.getId()).asSchemaConcept();

            assertEquals(localConcept.isSchemaConcept(), remoteConcept.isSchemaConcept());
            assertEquals(localConcept.isImplicit(), remoteConcept.isImplicit());
            assertEquals(localConcept.getLabel(), remoteConcept.getLabel());
            assertEquals(localConcept.sup().getId(), remoteConcept.sup().getId());
            assertEqualConcepts(localConcept, remoteConcept, SchemaConcept::sups);
            assertEqualConcepts(localConcept, remoteConcept, SchemaConcept::subs);
        }
    }

    @Ignore // TODO: implement missing methods
    @Test
    public void whenGettingAThing_TheThingContainsInformationAboutItself() {
        try (GraknTx remoteTx = remoteSession.open(GraknTxType.READ);
             GraknTx localTx = localSession.open(GraknTxType.READ)
        ) {
            GetQuery query = remoteTx.graql().match(var("x").isa("thing")).get();
            Thing remoteConcept = query.stream().findAny().get().get("x").asThing();
            Thing localConcept = localTx.getConcept(remoteConcept.getId()).asThing();

            assertEquals(localConcept.isThing(), remoteConcept.isThing());
            assertEquals(localConcept.isInferred(), remoteConcept.isInferred());
            assertEquals(localConcept.type().getId(), remoteConcept.type().getId());
            assertEqualConcepts(localConcept, remoteConcept, Thing::attributes);
            assertEqualConcepts(localConcept, remoteConcept, Thing::keys);
            assertEqualConcepts(localConcept, remoteConcept, Thing::plays);
            assertEqualConcepts(localConcept, remoteConcept, Thing::relationships);
        }
    }

    private <T extends Concept> void assertEqualConcepts(
            T concept1, T concept2, Function<T, Stream<? extends Concept>> function
    ) {
        assertEquals(
                function.apply(concept1).map(Concept::getId).collect(toSet()),
                function.apply(concept2).map(Concept::getId).collect(toSet())
        );
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
