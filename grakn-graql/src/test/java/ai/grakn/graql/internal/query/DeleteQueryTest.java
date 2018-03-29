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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class DeleteQueryTest {

    private static final VarPattern ENTITY = Graql.label(Schema.MetaSchema.ENTITY.getLabel());

    public static final Var x = var("x");
    public static final Var y = var("y");
    private QueryBuilder qb;

    @ClassRule
    public static final SampleKBContext movieKB = MovieKB.context();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private Match kurtz;
    private Match marlonBrando;
    private Match apocalypseNow;
    private Match kurtzCastRelation;

    @Before
    public void setUp() {
        qb = movieKB.tx().graql();

        kurtz = qb.match(x.has("name", "Colonel Walter E. Kurtz"));
        marlonBrando = qb.match(x.has("name", "Marlon Brando"));
        apocalypseNow = qb.match(x.has("title", "Apocalypse Now"));
        kurtzCastRelation =
                qb.match(var("a").rel("character-being-played", var().has("name", "Colonel Walter E. Kurtz")));
    }

    @After
    public void cleanUp(){
        movieKB.rollback();
    }

    @Test
    public void testDeleteMultiple() {
        qb.define(label("fake-type").sub(ENTITY)).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).toList();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type")).delete(x).execute();

        assertNotExists(qb, var().isa("fake-type"));
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

    @Test
    public void testDeleteAllRolePlayers() {
        ConceptId id = kurtzCastRelation.get("a").findFirst().get().getId();
        Match relation = qb.match(var().id(id));

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
    public void afterDeletingAllInstances_TheTypeCanBeUndefined() {
        Match movie = qb.match(x.isa("movie"));

        assertNotNull(movieKB.tx().getEntityType("movie"));
        assertExists(movie);

        movie.delete(x).execute();

        assertNotNull(movieKB.tx().getEntityType("movie"));
        assertNotExists(movie);

        qb.undefine(label("movie").sub("production")).execute();

        assertNull(movieKB.tx().getEntityType("movie"));
    }

    @Test
    public void whenDeletingMultipleVariables_AllVariablesGetDeleted() {
        qb.define(label("fake-type").sub(ENTITY)).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).toList();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type"), y.isa("fake-type"), x.neq(y)).limit(1).delete(x, y).execute();

        assertNotExists(qb, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingWithNoArguments_AllVariablesGetDeleted() {
        qb.define(label("fake-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue())).execute();
        qb.insert(x.isa("fake-type"), y.isa("fake-type")).toList();

        assertEquals(2, qb.match(x.isa("fake-type")).stream().count());

        qb.match(x.isa("fake-type"), y.isa("fake-type"), x.neq(y)).limit(1).delete().execute();

        assertNotExists(qb, var().isa("fake-type"));
    }

    @Test
    public void whenDeletingAVariableNotInTheQuery_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(y));
        movieKB.tx().graql().match(x.isa("movie")).delete(y).execute();
    }

    @Test
    public void whenDeletingASchemaConcept_Throw() {
        SchemaConcept newType = qb.define(x.label("new-type").sub(ENTITY)).execute().get(x).asSchemaConcept();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.deleteSchemaConcept(newType).getMessage());
        qb.match(x.label("new-type")).delete(x).execute();
    }

    @Test(expected = Exception.class)
    public void deleteVarNameNullSet() {
        movieKB.tx().graql().match(var()).delete((Set<Var>) null).execute();
    }

    @Test(expected = Exception.class)
    public void whenDeleteIsPassedNull_Throw() {
        movieKB.tx().graql().match(var()).delete((String) null).execute();
    }

}
