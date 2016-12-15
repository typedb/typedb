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

package ai.grakn.test.graql.printer;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.AbstractMovieGraphTest;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static ai.grakn.graql.Graql.var;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GraqlPrinterTest extends AbstractMovieGraphTest {

    @Test
    public void testRelationOutput() {
        Printer printer = Printers.graql();

        MatchQuery query = graph.graql().match(var("r").isa("has-cast")
                .rel(var().has("name", "Al Pacino"))
                .rel(var().has("name", "Michael Corleone"))
                .rel(var().has("title", "Godfather")));

        String relationString = printer.graqlString(query.get("r").iterator().next());

        assertThat(relationString, containsString("has-cast"));
        assertThat(relationString, containsString("actor"));
        assertThat(relationString, containsString("production-with-cast"));
        assertThat(relationString, containsString("character-being-played"));
    }

    @Test
    public void testResourceOutputNoResources() {
        Printer printer = Printers.graql();

        Instance godfather = graph.getResourceType("title").getResource("Godfather").owner();

        String repr = printer.graqlString(godfather);

        assertThat(
                repr,
                allOf(containsString("movie"), not(containsString("title")), not(containsString("Godfather")))
        );
    }

    @Test
    public void testResourceOutputWithResource() {
        Printer printer = Printers.graql(
                graph.getResourceType("title"), graph.getResourceType("tmdb-vote-count"), graph.getResourceType("name")
        );

        Instance godfather = graph.getResourceType("title").getResource("Godfather").owner();

        String repr = printer.graqlString(godfather);

        //noinspection unchecked
        assertThat(repr, allOf(
                containsString("movie"), containsString("has"), containsString("title"), containsString("\"Godfather\""),
                containsString("tmdb-vote-count"), containsString("1000"), not(containsString("name"))
        ));
    }

    @Test
    public void testEmptyResult() {
        Printer printer = Printers.graql();

        Map<String, Concept> emptyResult = Maps.newHashMap();

        assertEquals("{}", printer.graqlString(emptyResult));
    }

    @Test
    public void testType() {
        Printer printer = Printers.graql();

        Type production = graph.getEntityType("production");

        String productionString = printer.graqlString(production);

        assertThat(productionString, containsString("type-name"));
        assertThat(productionString, containsString("production"));
        assertThat(productionString, containsString("sub"));
        assertThat(productionString, containsString("entity"));
        assertThat(productionString, not(containsString("isa")));
        assertThat(productionString, not(containsString("entity-type")));
    }

    @Test
    public void testEntityType() {
        Printer printer = Printers.graql();

        Type entity = graph.admin().getMetaEntityType();

        String entityString = printer.graqlString(entity);

        assertThat(entityString, containsString("type-name"));
        assertThat(entityString, containsString("entity"));
        assertThat(entityString, containsString("sub"));
        assertThat(entityString, containsString("concept"));
        assertThat(entityString, not(containsString("isa")));
    }

    @Test
    public void testConcept() {
        Printer printer = Printers.graql();

        Type concept = graph.admin().getMetaConcept();

        String conceptString = printer.graqlString(concept);

        assertThat(conceptString, containsString("type-name"));
        assertThat(conceptString, containsString("concept"));
        assertThat(conceptString, not(containsString("sub")));
        assertThat(conceptString, not(containsString("isa")));
    }
}
