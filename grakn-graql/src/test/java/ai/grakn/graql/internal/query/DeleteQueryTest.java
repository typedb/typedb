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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.test.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Set;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.NO_PATTERNS;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class DeleteQueryTest {

    public static final Var x = var("x");
    public static final Var y = var("y");
    private QueryBuilder qb;

    @ClassRule
    public static final SampleKBContext movieGraph = SampleKBContext.preLoad(MovieKB.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private MatchQuery kurtz;
    private MatchQuery marlonBrando;
    private MatchQuery apocalypseNow;
    private MatchQuery kurtzCastRelation;

    @Before
    public void setUp() {
        qb = movieGraph.graph().graql();

        kurtz = qb.match(x.has("name", "Colonel Walter E. Kurtz"));
        marlonBrando = qb.match(x.has("name", "Marlon Brando"));
        apocalypseNow = qb.match(x.has("title", "Apocalypse Now"));
        kurtzCastRelation =
                qb.match(var("a").rel("character-being-played", var().has("name", "Colonel Walter E. Kurtz")));
    }

    @After
    public void cleanUp(){
        movieGraph.rollback();
    }

    @Test
    public void testDeleteMultiple() {
        qb.insert(label("fake-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue())).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).execute();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type")).delete(x).execute();

        assertNotExists(qb, var().isa("fake-type"));
    }

    @Test
    public void testDeleteName() {
        qb.insert(
                var().isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertExists(qb, var().isa("person").has("real-name", "Bob"));
        assertExists(qb, var().isa("person").has("real-name", "Robert"));
        assertExists(qb, var().isa("person").has("gender", "male"));

        qb.match(x.has("real-name", "Bob")).delete(x.has("real-name", y)).execute();

        assertNotExists(qb, var().isa("person").has("real-name", "Bob"));
        assertNotExists(qb, var().isa("person").has("real-name", "Robert"));
        assertExists(qb, var().isa("person").has("gender", "male"));

        qb.match(x.has("gender", "male")).delete(x).execute();
        assertNotExists(qb, var().has("gender", "male"));

        assertExists(qb, var().isa("real-name").val("Bob"));
        assertExists(qb, var().isa("real-name").val("Robert"));
        assertExists(qb, var().isa("gender").val("male"));
    }

    @Test
    public void testDeleteSpecificEdge() {
        VarPattern actor = label("has-cast").relates("actor");
        VarPattern productionWithCast = label("has-cast").relates("production-with-cast");

        assertExists(qb, actor);
        assertExists(qb, productionWithCast);

        qb.match(x.label("has-cast")).delete(x.relates("actor")).execute();
        assertExists(qb, label("has-cast"));
        assertNotExists(qb, actor);
        assertExists(qb, productionWithCast);

        qb.insert(actor).execute();
        assertExists(qb, actor);
    }

    @Test
    public void testDeleteSpecificName() {
        qb.insert(
                var().isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertExists(qb, var().isa("person").has("real-name", "Bob"));
        assertExists(qb, var().isa("person").has("real-name", "Robert"));
        assertExists(qb, var().isa("person").has("gender", "male"));

        qb.match(x.has("real-name", "Bob")).delete(x.has("real-name", "Robert")).execute();

        assertExists(qb, var().isa("person").has("real-name", "Bob"));
        assertNotExists(qb, var().isa("person").has("real-name", "Robert"));
        assertExists(qb, var().isa("person").has("gender", "male"));

        qb.match(x.has("real-name", "Bob")).delete(x).execute();
        assertNotExists(qb, var().has("real-name", "Bob").isa("person"));

        assertExists(qb, var().isa("real-name").val("Bob"));
        assertExists(qb, var().isa("real-name").val("Robert"));
        assertExists(qb, var().isa("gender").val("male"));
    }

    @Test
    public void testDeleteEntity() {

        assertExists(qb, var().has("title", "Godfather"));
        assertExists(qb, x.has("title", "Godfather"), var().rel(x).rel(y).isa("has-cast"));
        assertExists(qb, var().has("name", "Don Vito Corleone"));

        qb.match(x.has("title", "Godfather")).delete(x).execute();

        assertNotExists(qb, var().has("title", "Godfather"));
        assertNotExists(qb, x.has("title", "Godfather"), var().rel(x).rel(y).isa("has-cast"));
        assertExists(qb, var().has("name", "Don Vito Corleone"));
    }

    @Test
    public void testDeleteRelation() {
        assertExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(kurtzCastRelation);

        kurtzCastRelation.delete("a").execute();

        assertExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertNotExists(kurtzCastRelation);
    }

    // TODO: Fix this scenario (test is fine, implementation is wrong!)
    @Test
    public void testDeleteAllRolePlayers() {
        ConceptId id = kurtzCastRelation.get("a").findFirst().get().getId();
        MatchQuery relation = qb.match(var().id(id));

        assertExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(relation);

        kurtz.delete(x).execute();

        assertNotExists(kurtz);
        assertExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(relation);

        marlonBrando.delete(x).execute();

        assertNotExists(kurtz);
        assertNotExists(marlonBrando);
        assertExists(apocalypseNow);
        assertExists(relation);

        apocalypseNow.delete(x).execute();

        assertNotExists(kurtz);
        assertNotExists(marlonBrando);
        assertNotExists(apocalypseNow);
        assertNotExists(relation);
    }

    @Test
    public void whenDeletingAResource_TheResourceAndImplicitRelationsAreDeleted() {
        ConceptId id = qb.match(
                x.has("title", "Godfather"),
                var("a").rel(x).rel(y).isa(Schema.ImplicitType.HAS.getLabel("tmdb-vote-count").getValue())
        ).get("a").findFirst().get().getId();

        assertExists(qb, var().has("title", "Godfather"));
        assertExists(qb, var().id(id));
        assertExists(qb, var().val(1000L).isa("tmdb-vote-count"));

        qb.match(x.val(1000L).isa("tmdb-vote-count")).delete(x).execute();

        assertExists(qb, var().has("title", "Godfather"));
        assertNotExists(qb, var().id(id));
        assertNotExists(qb, var().val(1000L).isa("tmdb-vote-count"));
    }

    @Test
    public void testDeleteEntityTypeWithNoInstances() {
        MatchQuery shoeType = qb.match(x.label("shoe").sub("entity"));

        qb.insert(label("shoe").sub("entity")).execute();

        assertExists(shoeType);

        shoeType.delete(x).execute();

        assertNotExists(shoeType);
    }

    @Test
    public void testDeleteEntityTypeAfterInstances() {
        MatchQuery movie = qb.match(x.isa("movie"));

        assertNotNull(movieGraph.graph().getEntityType("movie"));
        assertExists(movie);

        movie.delete(x).execute();

        assertNotNull(movieGraph.graph().getEntityType("movie"));
        assertNotExists(movie);

        qb.match(x.label("movie").sub("entity")).delete(x).execute();

        assertNull(movieGraph.graph().getEntityType("movie"));
    }

    @Test
    public void whenDeletingMultipleVariables_AllVariablesGetDeleted() {
        qb.insert(label("fake-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue())).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).execute();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type"), y.isa("fake-type"), x.neq(y)).limit(1).delete(x, y).execute();

        assertNotExists(qb, var().isa("fake-type"));
    }

    @Test
    public void testErrorWhenDeleteEntityTypeWithInstances() {
        assertExists(qb, x.label("movie").sub("entity"));
        assertExists(qb, x.isa("movie"));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(allOf(containsString("movie"), containsString("delet")));
        qb.match(x.label("movie").sub("entity")).delete(x).execute();
    }

    @Test
    public void testErrorWhenDeleteSuperEntityType() {
        assertExists(qb, x.label("production").sub("entity"));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(allOf(containsString("production"), containsString("delet")));
        qb.match(x.label("production").sub("entity")).delete(x).execute();
    }

    @Test
    public void testErrorWhenDeleteRoleTypeWithPlayers() {
        assertExists(qb, x.label("actor"));

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(allOf(containsString("actor"), containsString("delet")));
        qb.match(x.label("actor")).delete(x).execute();
    }

    @Test
    public void testErrorWhenDeleteValue() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("delet"), containsString("val")));
        qb.match(x.isa("movie")).delete(x.val("hello")).execute();
    }

    @Test
    public void whenDeletingAVariableNotInTheQuery_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(y));
        movieGraph.graph().graql().match(x.isa("movie")).delete(y).execute();
    }

    @Test
    public void whenDeletingAnEmptyPattern_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        movieGraph.graph().graql().match(var()).delete(Collections.EMPTY_SET).execute();
    }

    @Test(expected = Exception.class)
    public void deleteVarNameNullSet() {
        movieGraph.graph().graql().match(var()).delete((Set<VarPattern>) null).execute();
    }

    @Test(expected = Exception.class)
    public void whenDeleteIsPassedNull_Throw() {
        movieGraph.graph().graql().match(var()).delete((String) null).execute();
    }

}
