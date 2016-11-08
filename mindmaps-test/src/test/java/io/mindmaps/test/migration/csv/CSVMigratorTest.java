/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published kemby
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test.migration.csv;

import com.google.common.io.Files;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.migration.base.LoadingMigrator;
import io.mindmaps.migration.csv.CSVMigrator;
import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static java.util.stream.Collectors.joining;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CSVMigratorTest extends AbstractMindmapsMigratorTest {

    private LoadingMigrator migrator;
    private CSVMigrator csvMigrator;

    @Before
    public void setup(){
        BlockingLoader loader = new BlockingLoader(graph.getKeyspace());
        loader.setExecutorSize(1);
        loader.setBatchSize(10);

        csvMigrator = new CSVMigrator();
        migrator = new LoadingMigrator(loader, csvMigrator);
    }

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

        migrator.migrate(pokemonTemplate, getFile("csv", "multi-file/data/pokemon.csv"));
        migrator.migrate(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv"));
        migrator.migrate(edgeTemplate, getFile("csv", "multi-file/data/edges.csv"));

        Collection<Entity> pokemon = graph.getEntityType("pokemon").instances();
        assertEquals(9, pokemon.size());

        ResourceType<String> typeid = graph.getResourceType("type-id");
        ResourceType<String> pokedexno = graph.getResourceType("pokedex-no");

        Entity grass = graph.getResource("12", typeid).ownerInstances().iterator().next().asEntity();
        Entity poison = graph.getResource("4", typeid).ownerInstances().iterator().next().asEntity();
        Entity bulbasaur = graph.getResource("1", pokedexno).ownerInstances().iterator().next().asEntity();
        RelationType relation = graph.getRelationType("has-type");

        assertNotNull(grass);
        assertNotNull(poison);
        assertNotNull(bulbasaur);

        assertRelationBetweenInstancesExists(bulbasaur, grass, relation.getId());
        assertRelationBetweenInstancesExists(bulbasaur, poison, relation.getId());
    }

    @Test
    public void quotesWithoutContentTest() throws IOException {
        load(getFile("csv", "pets/schema.gql"));
        String template = Files.readLines(getFile("csv", "pets/template.gql"), StandardCharsets.UTF_8).stream().collect(joining("\n"));
        migrator.migrate(template, getFile("csv", "pets/quotes/emptyquotes.csv"));

        Collection<Entity> pets = graph.getEntityType("pet").instances();
        assertEquals(9, pets.size());

        Collection<Entity> cats = graph.getEntityType("cat").instances();
        assertEquals(2, cats.size());

        // test empty value not created
        ResourceType<String> name = graph.getResourceType("name");
        ResourceType<String> death = graph.getResourceType("death");

        Entity puffball = graph.getResource("Puffball", name).ownerInstances().iterator().next().asEntity();
        assertEquals(0, puffball.resources(death).size());

        Entity bowser = graph.getResource("Bowser", name).ownerInstances().iterator().next().asEntity();
        assertEquals(1, bowser.resources(death).size());
    }

    @Test
    @Ignore
    public void multipleEntitiesInOneFileTest() throws IOException {
        load(getFile("csv", "single-file/schema.gql"));
        assertNotNull(graph.getEntityType("make"));

        String template = Files.readLines(getFile("csv", "single-file/template.gql"), StandardCharsets.UTF_8).stream().collect(joining("\n"));
        migrator.migrate(template, getFile("csv", "single-file/data/cars.csv"));

        // test
        Collection<Entity> makes = graph.getEntityType("make").instances();
        assertEquals(3, makes.size());

        Collection<Entity> models = graph.getEntityType("model").instances();
        assertEquals(4, models.size());

        // test empty value not created
        ResourceType description = graph.getResourceType("description");

        Entity venture = graph.getEntity("Venture");
        assertEquals(1, venture.resources(description).size());

        Entity ventureLarge = graph.getEntity("Venture Large");
        assertEquals(0, ventureLarge.resources(description).size());
    }

    @Test
    public void testMigrateAsStringMethod(){
        load(getFile("csv", "multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTypeTemplate = "insert $x isa pokemon-type has type-id <id>-type has description <identifier>;";
        String templated = csvMigrator.migrate(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv"))
                .map(InsertQuery::toString)
                .collect(joining("\n"));

        System.out.println(templated);

        String expected = "id \"17-type\"";
        assertTrue(templated.contains(expected));
    }
}
