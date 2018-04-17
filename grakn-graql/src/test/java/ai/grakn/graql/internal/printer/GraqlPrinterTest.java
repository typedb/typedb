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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.Relationship;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Match;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.util.Schema;
import org.apache.commons.lang.StringUtils;
import org.junit.ClassRule;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GraqlPrinterTest {

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @Test
    public void testRelationOutput() {
        Printer printer = Printers.graql(true);

        Match match = rule.tx().graql().match(var("r").isa("has-cast")
                .rel(var().has("name", "Al Pacino"))
                .rel(var().has("name", "Michael Corleone"))
                .rel(var().has("title", "Godfather")));

        String relationString = printer.graqlString(match.get("r").iterator().next());

        assertThat(relationString, containsString("has-cast"));
        assertThat(relationString, containsString("actor"));
        assertThat(relationString, containsString("production-with-cast"));
        assertThat(relationString, containsString("character-being-played"));
    }

    @Test
    public void whenGettingOutputForRelation_TheResultShouldHaveCommasBetweenRolePlayers() {
        Printer printer = Printers.graql(true);

        Match match = rule.tx().graql().match(var("r").isa("has-cluster"));

        Relationship relationship = match.get("r").iterator().next().asRelationship();
        long numRolePlayers = relationship.rolePlayers().count();
        long numCommas = numRolePlayers - 1;

        String relationString = printer.graqlString(relationship);

        assertEquals(
                relationString + " should have " + numCommas + " commas separating role-players",
                numCommas, StringUtils.countMatches(relationString, ",")
        );
    }

    @Test
    public void whenGettingOutputForResource_IncludesValueOfResource() {
        Printer printer = Printers.graql(false);

        Match match = rule.tx().graql().match(var("x").isa("title").val("Godfather"));

        String result = printer.graqlString(match.iterator().next());

        assertEquals("$x val \"Godfather\" isa title;", result.trim());
    }

    @Test
    public void testResourceOutputNoResources() {
        Printer printer = Printers.graql(true);

        Thing godfather = rule.tx().getAttributeType("title").getAttribute("Godfather").owner();

        String repr = printer.graqlString(godfather);

        assertThat(
                repr,
                allOf(containsString("movie"), not(containsString("title")), not(containsString("Godfather")))
        );
    }

    // allOf accepts an array with generics
    @SuppressWarnings("unchecked")
    @Test
    public void testResourceOutputWithResource() {
        Printer printer = Printers.graql(
                true, rule.tx().getAttributeType("title"), rule.tx().getAttributeType("tmdb-vote-count"), rule.tx().getAttributeType("name")
        );

        Thing godfather = rule.tx().getAttributeType("title").getAttribute("Godfather").owner();

        String repr = printer.graqlString(godfather);

        assertThat(repr, allOf(
                containsString("movie"), containsString("has"), containsString("title"), containsString("\"Godfather\""),
                containsString("tmdb-vote-count"), containsString("1000"), not(containsString("name"))
        ));
    }

    @Test
    public void testEmptyResult() {
        Printer printer = Printers.graql(true);

        Answer emptyResult = new QueryAnswer();

        assertEquals("{}", printer.graqlString(emptyResult));
    }

    @Test
    public void testType() {
        Printer printer = Printers.graql(true);

        Type production = rule.tx().getEntityType("production");

        String productionString = printer.graqlString(production);

        assertThat(productionString, containsString("label"));
        assertThat(productionString, containsString("production"));
        assertThat(productionString, containsString("sub"));
        assertThat(productionString, containsString("entity"));
        assertThat(productionString, not(containsString("isa")));
        assertThat(productionString, not(containsString("entity-type")));
    }

    @Test
    public void testEntityType() {
        Printer printer = Printers.graql(true);

        Type entity = rule.tx().admin().getMetaEntityType();

        String entityString = printer.graqlString(entity);

        assertThat(entityString, containsString("label"));
        assertThat(entityString, containsString("entity"));
        assertThat(entityString, containsString("sub"));
        assertThat(entityString, containsString(Schema.MetaSchema.THING.getLabel().getValue()));
        assertThat(entityString, not(containsString("isa")));
    }

    @Test
    public void testConcept() {
        Printer printer = Printers.graql(true);

        SchemaConcept concept = rule.tx().admin().getMetaConcept();

        String conceptString = printer.graqlString(concept);

        assertThat(conceptString, containsString("label"));
        assertThat(conceptString, containsString(Schema.MetaSchema.THING.getLabel().getValue()));
        assertThat(conceptString, not(containsString("sub")));
        assertThat(conceptString, not(containsString("isa")));
    }

    @Test
    public void whenPrintingRole_ShowLabel() {
        Printer printer = Printers.graql(true);

        Role role = rule.tx().admin().getMetaRole();

        String roleString = printer.graqlString(role);

        assertThat(roleString, containsString("role"));
    }

    @Test
    public void whenPrintingWithColorizeTrue_ResultIsColored(){
        Printer printer = Printers.graql(true);

        Type production = rule.tx().getEntityType("production");

        String productionString = printer.graqlString(production);
        assertThat(productionString, containsString("\u001B"));
    }

    @Test
    public void whenPrintingWitholorizeFalse_ResultIsNotColored(){
        Printer printer = Printers.graql(false);

        Type production = rule.tx().getEntityType("production");

        String productionString = printer.graqlString(production);
        assertThat(productionString, not(containsString("\u001B")));
    }

    @Test
    public void whenPrintingNull_ResultIsNullString() {
        Printer printer = Printers.graql(false);

        assertEquals("null", printer.graqlString(null));
    }
}
