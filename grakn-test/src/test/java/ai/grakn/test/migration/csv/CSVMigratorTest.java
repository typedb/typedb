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

package ai.grakn.test.migration.csv;

import ai.grakn.concept.Entity;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.InsertQuery;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.test.migration.AbstractGraknMigratorTest;
import com.google.common.io.Files;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static java.util.stream.Collectors.joining;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CSVMigratorTest extends AbstractGraknMigratorTest {

    @Test
    public void multiFileMigrateGraphPersistenceTest(){
        load(getFile("csv", "multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTemplate = "" +
                "insert $x isa pokemon                      " +
                "    has description <identifier>  \n" +
                "    has pokedex-no <id>           \n" +
                "    has height @int(height)       \n" +
                "    has weight @int(weight);        ";

        String pokemonTypeTemplate = "               " +
                "insert $x isa pokemon-type                 " +
                "   has type-id <id>                 " +
                "   has description <identifier>;    ";

        String edgeTemplate = "" +
                "match                                            " +
                "   $pokemon has pokedex-no <pokemon_id>        ; " +
                "   $type has type-id <type_id>                 ; " +
                "insert (pokemon-with-type: $pokemon, type-of-pokemon: $type) isa has-type;";

        migrate(new CSVMigrator(pokemonTemplate, getFile("csv", "multi-file/data/pokemon.csv")));
        migrate(new CSVMigrator(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv")));
        migrate(new CSVMigrator(edgeTemplate, getFile("csv", "multi-file/data/edges.csv")));

        assertPokemonGraphCorrect();
    }

    @Test
    public void quotesWithoutContentTest() throws IOException {
        load(getFile("csv", "pets/schema.gql"));
        String template = Files.readLines(getFile("csv", "pets/template.gql"), StandardCharsets.UTF_8).stream().collect(joining("\n"));
        migrate(new CSVMigrator(template, getFile("csv", "pets/quotes/emptyquotes.csv")));
        assertPetGraphCorrect();
    }

    @Test
    @Ignore
    public void multipleEntitiesInOneFileTest() throws IOException {
        load(getFile("csv", "single-file/schema.gql"));
        assertNotNull(graph.getEntityType("make"));

        String template = Files.readLines(getFile("csv", "single-file/template.gql"), StandardCharsets.UTF_8).stream().collect(joining("\n"));
        migrate(new CSVMigrator(template, getFile("csv", "single-file/data/cars.csv")));

        // test
        Collection<Entity> makes = graph.getEntityType("make").instances();
        assertEquals(3, makes.size());

        Collection<Entity> models = graph.getEntityType("model").instances();
        assertEquals(4, models.size());

        // test empty value not created
        ResourceType description = graph.getResourceType("description");

        Entity venture = graph.getConcept("Venture");
        assertEquals(1, venture.resources(description).size());

        Entity ventureLarge = graph.getConcept("Venture Large");
        assertEquals(0, ventureLarge.resources(description).size());
    }

    @Test
    public void testMigrateAsStringMethod(){
        load(getFile("csv", "multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTypeTemplate = "insert $x isa pokemon-type has type-id <id>-type has description <identifier>;";
        String templated = new CSVMigrator(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv")).migrate()
                .map(InsertQuery::toString)
                .collect(joining("\n"));

        String expected = "id \"17-type\"";
        assertTrue(templated.contains(expected));
    }
}
