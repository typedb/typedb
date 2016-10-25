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

import io.mindmaps.concept.Entity;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.graql.Var;
import io.mindmaps.migration.base.LoadingMigrator;
import io.mindmaps.migration.csv.CSVMigrator;
import io.mindmaps.test.migration.AbstractMindmapsMigratorTest;
import org.junit.Before;
import org.junit.Test;

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

        csvMigrator = new CSVMigrator();
        csvMigrator.setBatchSize(50);

        migrator = new LoadingMigrator(loader, csvMigrator);
    }

    @Test
    public void multiFileMigrateGraphPersistenceTest(){
        load(getFile("csv", "multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTemplate = "" +
                "$x isa pokemon id <id>-pokemon \n" +
                "    has description <identifier>\n" +
                "    has pokedex-no @int(id)\n" +
                "    has height @int(height)\n" +
                "    has weight @int(weight);";

        String pokemonTypeTemplate = "$x isa pokemon-type id <id>-type has description <identifier>;";

        String edgeTemplate = "(pokemon-with-type: <pokemon_id>-pokemon, type-of-pokemon: <type_id>-type) isa has-type;";

        migrator.migrate(pokemonTemplate, getFile("csv", "multi-file/data/pokemon.csv"));

        Collection<Entity> pokemon = graph.getEntityType("pokemon").instances();
        assertEquals(151, pokemon.size());

        migrator.migrate(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv"));

        Collection<Entity> types = graph.getEntityType("pokemon-type").instances();
        assertEquals(20, types.size());

        migrator.migrate(edgeTemplate, getFile("csv", "multi-file/data/edges.csv"));

        Entity grass = graph.getEntity("12-type");
        Entity poison = graph.getEntity("4-type");
        Entity bulbasaur = graph.getEntity("1-pokemon");
        RelationType relation = graph.getRelationType("has-type");

        assertRelationBetweenInstancesExists(bulbasaur, grass, relation.getId());
        assertRelationBetweenInstancesExists(bulbasaur, poison, relation.getId());
    }

    @Test
    public void multipleEntitiesInOneFileTest(){
        load(getFile("csv", "single-file/schema.gql"));
        assertNotNull(graph.getEntityType("make"));

        String template = "" +
                "$x isa make id <Make>;\n" +
                "$y isa model id <Model>\n" +
                "    if (ne Year null) do {has year <Year> }\n " +
                "    if (ne Description null) do { has description <Description> }\n" +
                "    if (ne Price null) do { has price @double(Price) ;\n" +
                "(make-of-car: $x, model-of-car: $y) isa make-and-model;";

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

        String pokemonTypeTemplate = "$x isa pokemon-type id <id>-type has description <identifier>;";
        String templated = csvMigrator.migrate(pokemonTypeTemplate, getFile("csv", "multi-file/data/types.csv"))
                .flatMap(Collection::stream)
                .map(Var::toString)
                .collect(joining("\n"));

        String expected = "id \"17-type\"";
        assertTrue(templated.contains(expected));
    }
}
