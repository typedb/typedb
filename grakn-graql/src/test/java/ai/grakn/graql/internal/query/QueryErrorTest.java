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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.INVALID_VALUE;
import static ai.grakn.util.ErrorMessage.NO_PATTERNS;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;

public class QueryErrorTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @ClassRule
    public static final SampleKBContext empty = SampleKBContext.empty();

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = rule.tx().graql();
    }

    @Test
    public void testErrorNonExistentConceptType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("film");
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("x").isa("film")).stream();
    }

    @Test
    public void testErrorNotARole() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("role"), containsString("person"), containsString("isa person")));
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("x").isa("movie"), var().rel("person", "y").rel("x")).stream();
    }

    @Test
    public void testErrorNonExistentResourceType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("thingy");
        qb.match(var("x").has("thingy", "value")).delete("x").execute();
    }

    @Test
    public void whenMatchingWildcardHas_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.noLabelSpecifiedForHas(var("x")).getMessage());
        qb.match(label("thing").has(var("x"))).get().toList();
    }

    @Test
    public void whenMatchingHasWithNonExistentType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.labelNotFound(Label.of("heffalump")).getMessage());
        qb.match(var("x").has("heffalump", "foo")).get().toList();
    }

    @Test
    public void testErrorNotARelation() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(
                containsString("relation"), containsString("movie"), containsString("separate"), containsString(";")));
        //noinspection ResultOfMethodCallIgnored
        qb.match(var().isa("movie").rel("x").rel("y")).stream();
    }

    @Test
    public void testErrorInvalidNonExistentRole() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.NOT_A_ROLE_TYPE.getMessage("character-in-production", "character-in-production"));
        //noinspection ResultOfMethodCallIgnored
        qb.match(var().isa("has-cast").rel("character-in-production", "x")).stream();
    }

    @Test
    public void testErrorMultipleIsa() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("isa"), containsString("person"), containsString("has-cast")
        ));
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("abc").isa("person").isa("has-cast"));
    }

    @Test
    public void whenSpecifyingMultipleSubs_ThrowIncludingInformationAboutTheConceptAndBothSupers() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(
                containsString("abc"), containsString("sub"), containsString("person"), containsString("has-cast")
        ));
        qb.define(label("abc").sub("person"), label("abc").sub("has-cast")).execute();
    }

    @Test
    public void testErrorHasGenreQuery() {
        // 'has genre' is not allowed because genre is an entity type
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.MUST_BE_ATTRIBUTE_TYPE.getMessage("genre"));
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("x").isa("movie").has("genre", "Drama")).stream();
    }

    @Test
    public void testExceptionWhenNoPatternsProvided() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(NO_PATTERNS.getMessage());
        //noinspection ResultOfMethodCallIgnored
        qb.match();
    }

    @Test
    public void testExceptionWhenNullValue() {
        exception.expect(NullPointerException.class);
        //noinspection ResultOfMethodCallIgnored
        var("x").val(null);
    }

    @Test
    public void testExceptionWhenNoHasResourceRelation() throws InvalidKBException {
        // Create a fresh graph, with no has between person and name
        QueryBuilder emptyQb = empty.tx().graql();
        emptyQb.define(
                label("person").sub("entity"),
                label("name").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING)
        ).execute();

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(allOf(
                containsString("person"),
                containsString("name")
        ));
        emptyQb.insert(var().isa("person").has("name", "Bob")).toList();
    }

    @Test
    public void testExceptionInstanceOfRoleType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.cannotGetInstancesOfNonType(Label.of("actor")).getMessage());
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("x").isa("actor")).stream();
    }

    @Test
    public void testExceptionInstanceOfRule() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.cannotGetInstancesOfNonType(Label.of("rule")).getMessage());
        //noinspection ResultOfMethodCallIgnored
        qb.match(var("x").isa("rule")).stream();
    }

    @Test
    public void testAdditionalSemicolon() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("id"), containsString("plays product-type")));
        qb.parse(
                "define " +
                        "tag-group sub role; product-type sub role;" +
                        "category sub entity, plays tag-group; plays product-type;"
        ).execute();
    }

    @Test
    public void testGetNonExistentVariable() {
        Match match = qb.match(var("x").isa("movie"));

        Stream<Concept> concepts = match.get("y");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.VARIABLE_NOT_IN_QUERY.getMessage(Graql.var("y")));

        //noinspection ResultOfMethodCallIgnored
        concepts.count();
    }

    @Test
    public void whenUsingInvalidResourceValue_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(INVALID_VALUE.getMessage(qb.getClass()));
        qb.match(var("x").val(qb));
    }

    @Test
    public void whenTryingToSetExistingInstanceType_Throw() {
        Thing movie = rule.tx().getEntityType("movie").instances().iterator().next();
        Type person = rule.tx().getEntityType("person");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(containsString("person"));

        qb.match(var("x").id(movie.getId())).insert(var("x").isa(label(person.getLabel()))).toList();
    }
}
