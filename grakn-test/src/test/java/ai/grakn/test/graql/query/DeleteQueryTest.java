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

package ai.grakn.test.graql.query;

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.ConceptException;
import ai.grakn.graphs.MovieGraph;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.test.GraphContext;
import ai.grakn.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class DeleteQueryTest {

    private QueryBuilder qb;

    @ClassRule
    public static final GraphContext movieGraph = GraphContext.preLoad(MovieGraph.get());

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private MatchQuery kurtz;
    private MatchQuery marlonBrando;
    private MatchQuery apocalypseNow;
    private MatchQuery kurtzCastRelation;

    @Before
    public void setUp() {
        qb = movieGraph.graph().graql();

        kurtz = qb.match(var("x").has("name", "Colonel Walter E. Kurtz"));
        marlonBrando = qb.match(var("x").has("name", "Marlon Brando"));
        apocalypseNow = qb.match(var("x").has("title", "Apocalypse Now"));
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
        qb.insert(var("x").isa("fake-type"), var("y").isa("fake-type")).execute();

        assertEquals(2, qb.match(var("x").isa("fake-type")).stream().count());

        qb.match(var("x").isa("fake-type")).delete("x").execute();

        assertFalse(qb.match(var().isa("fake-type")).ask().execute());
    }

    @Test
    public void testDeleteName() {
        qb.insert(
                var().isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("real-name", "Bob")).delete(var("x").has("real-name", var("y"))).execute();

        assertFalse(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("gender", "male")).delete("x").execute();
        assertFalse(qb.match(var().has("gender", "male")).ask().execute());

        assertTrue(qb.match(var().isa("real-name").val("Bob")).ask().execute());
        assertTrue(qb.match(var().isa("real-name").val("Robert")).ask().execute());
        assertTrue(qb.match(var().isa("gender").val("male")).ask().execute());
    }

    @Test
    public void testDeleteSpecificEdge() {
        Var actor = label("has-cast").relates("actor");
        Var productionWithCast = label("has-cast").relates("production-with-cast");

        assertTrue(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.match(var("x").label("has-cast")).delete(var("x").relates("actor")).execute();
        assertTrue(qb.match(label("has-cast")).ask().execute());
        assertFalse(qb.match(actor).ask().execute());
        assertTrue(qb.match(productionWithCast).ask().execute());

        qb.insert(actor).execute();
        assertTrue(qb.match(actor).ask().execute());
    }

    @Test
    public void testDeleteSpecificName() {
        qb.insert(
                var().isa("person")
                        .has("real-name", "Bob")
                        .has("real-name", "Robert")
                        .has("gender", "male")
        ).execute();

        assertTrue(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("real-name", "Bob")).delete(var("x").has("real-name", "Robert")).execute();

        assertTrue(qb.match(var().isa("person").has("real-name", "Bob")).ask().execute());
        assertFalse(qb.match(var().isa("person").has("real-name", "Robert")).ask().execute());
        assertTrue(qb.match(var().isa("person").has("gender", "male")).ask().execute());

        qb.match(var("x").has("real-name", "Bob")).delete("x").execute();
        assertFalse(qb.match(var().has("real-name", "Bob").isa("person")).ask().execute());

        assertTrue(qb.match(var().isa("real-name").val("Bob")).ask().execute());
        assertTrue(qb.match(var().isa("real-name").val("Robert")).ask().execute());
        assertTrue(qb.match(var().isa("gender").val("male")).ask().execute());
    }

    @Test
    public void testDeleteEntity() {
        AskQuery godfatherExists = qb.match(var().has("title", "Godfather")).ask();
        AskQuery godfatherHasRelations = qb.match(
                var("x").has("title", "Godfather"),
                var().rel("x").rel("y").isa("has-cast")
        ).ask();
        AskQuery donVitoCorleoneExists = qb.match(var().has("name", "Don Vito Corleone")).ask();

        assertTrue(godfatherExists.execute());
        assertTrue(godfatherHasRelations.execute());
        assertTrue(donVitoCorleoneExists.execute());

        qb.match(var("x").has("title", "Godfather")).delete("x").execute();

        assertFalse(godfatherExists.execute());
        assertFalse(godfatherHasRelations.execute());
        assertTrue(donVitoCorleoneExists.execute());
    }

    @Test
    public void testDeleteRelation() {
        assertTrue(exists(kurtz));
        assertTrue(exists(marlonBrando));
        assertTrue(exists(apocalypseNow));
        assertTrue(exists(kurtzCastRelation));

        kurtzCastRelation.delete("a").execute();

        assertTrue(exists(kurtz));
        assertTrue(exists(marlonBrando));
        assertTrue(exists(apocalypseNow));
        assertFalse(exists(kurtzCastRelation));
    }

    // TODO: Fix this scenario (test is fine, implementation is wrong!)
    @Test
    public void testDeleteAllRolePlayers() {
        ConceptId id = kurtzCastRelation.get("a").findFirst().get().getId();
        MatchQuery relation = qb.match(var().id(id));

        assertTrue(exists(kurtz));
        assertTrue(exists(marlonBrando));
        assertTrue(exists(apocalypseNow));
        assertTrue(exists(relation));

        kurtz.delete("x").execute();

        assertFalse(exists(kurtz));
        assertTrue(exists(marlonBrando));
        assertTrue(exists(apocalypseNow));
        assertTrue(exists(relation));

        marlonBrando.delete("x").execute();

        assertFalse(exists(kurtz));
        assertFalse(exists(marlonBrando));
        assertTrue(exists(apocalypseNow));
        assertTrue(exists(relation));

        apocalypseNow.delete("x").execute();

        assertFalse(exists(kurtz));
        assertFalse(exists(marlonBrando));
        assertFalse(exists(apocalypseNow));
        assertFalse(exists(relation));
    }

    @Test
    public void testDeleteResource() {
        MatchQuery godfather = qb.match(var().has("title", "Godfather"));
        ConceptId id = qb.match(
                var("x").has("title", "Godfather"),
                var("a").rel("x").rel("y").isa(Schema.ImplicitType.HAS.getLabel("tmdb-vote-count").getValue())
        ).get("a").findFirst().get().getId();
        MatchQuery relation = qb.match(var().id(id));
        MatchQuery voteCount = qb.match(var().val(1000L).isa("tmdb-vote-count"));

        assertTrue(exists(godfather));
        assertTrue(exists(relation));
        assertTrue(exists(voteCount));

        qb.match(var("x").val(1000L).isa("tmdb-vote-count")).delete("x").execute();

        assertTrue(exists(godfather));
        assertFalse(exists(relation)); //Relation is implicit it was deleted
        assertFalse(exists(voteCount));
    }

    @Test
    public void testDeleteEntityTypeWithNoInstances() {
        MatchQuery shoeType = qb.match(var("x").label("shoe").sub("entity"));

        qb.insert(label("shoe").sub("entity")).execute();

        assertTrue(exists(shoeType));

        shoeType.delete("x").execute();

        assertFalse(exists(shoeType));
    }

    @Test
    public void testDeleteEntityTypeAfterInstances() {
        MatchQuery movie = qb.match(var("x").isa("movie"));

        assertNotNull(movieGraph.graph().getEntityType("movie"));
        assertTrue(exists(movie));

        movie.delete("x").execute();

        assertNotNull(movieGraph.graph().getEntityType("movie"));
        assertFalse(exists(movie));

        qb.match(var("x").label("movie").sub("entity")).delete("x").execute();

        assertNull(movieGraph.graph().getEntityType("movie"));
    }

    @Test
    public void testErrorWhenDeleteEntityTypeWithInstances() {
        MatchQuery movieType = qb.match(var("x").label("movie").sub("entity"));
        MatchQuery movie = qb.match(var("x").isa("movie"));

        assertTrue(exists(movieType));
        assertTrue(exists(movie));

        exception.expect(ConceptException.class);
        exception.expectMessage(allOf(containsString("movie"), containsString("delet")));
        movieType.delete("x").execute();
    }

    @Test
    public void testErrorWhenDeleteSuperEntityType() {
        MatchQuery productionType = qb.match(var("x").label("production").sub("entity"));

        assertTrue(exists(productionType));

        exception.expect(ConceptException.class);
        exception.expectMessage(allOf(containsString("production"), containsString("delet")));
        productionType.delete("x").execute();
    }

    @Test
    public void testErrorWhenDeleteRoleTypeWithPlayers() {
        MatchQuery actor = qb.match(var("x").label("actor"));

        assertTrue(exists(actor));

        exception.expect(ConceptException.class);
        exception.expectMessage(allOf(containsString("actor"), containsString("delet")));
        actor.delete("x").execute();
    }

    @Test
    public void testErrorWhenDeleteValue() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage(allOf(containsString("delet"), containsString("val")));
        qb.match(var("x").isa("movie")).delete(var("x").val("hello")).execute();
    }

    private boolean exists(MatchQuery query) {
        return query.ask().execute();
    }
}
