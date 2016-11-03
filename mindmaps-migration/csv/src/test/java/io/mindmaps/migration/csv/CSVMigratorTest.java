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

package io.mindmaps.migration.csv;

import com.google.common.io.Files;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.Var;
import io.mindmaps.migration.base.LoadingMigrator;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CSVMigratorTest {

    private MindmapsGraph graph;
    private LoadingMigrator migrator;
    private CSVMigrator csvMigrator;

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        MindmapsEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        MindmapsEngineServer.stop();
    }

    @Before
    public void setup(){
        String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);

        BlockingLoader loader = new BlockingLoader(GRAPH_NAME);
        loader.setExecutorSize(1);
        loader.setBatchSize(10);

        csvMigrator = new CSVMigrator();
        migrator = new LoadingMigrator(loader, csvMigrator);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void multiFileMigrateGraphPersistenceTest(){
        load(getFile("multi-file/schema.gql"));
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

        migrate(pokemonTemplate, getFile("multi-file/data/pokemon.csv"));
        migrate(pokemonTypeTemplate, getFile("multi-file/data/types.csv"));
        migrate(edgeTemplate, getFile("multi-file/data/edges.csv"));

        Collection<Entity> pokemon = graph.getEntityType("pokemon").instances();
        assertEquals(151, pokemon.size());

        ResourceType<String> typeid = graph.getResourceType("type-id");
        ResourceType<String> pokedexno = graph.getResourceType("pokedex-no");

        Entity grass = graph.getResource("12", typeid).ownerInstances().iterator().next().asEntity();
        Entity poison = graph.getResource("4", typeid).ownerInstances().iterator().next().asEntity();
        Entity bulbasaur = graph.getResource("1", pokedexno).ownerInstances().iterator().next().asEntity();
        RelationType relation = graph.getRelationType("has-type");

        assertNotNull(grass);
        assertNotNull(poison);
        assertNotNull(bulbasaur);

        assertRelationExists(relation, bulbasaur, grass);
        assertRelationExists(relation, bulbasaur, poison);
    }

    @Test
    public void quotesWithoutContentTest() throws IOException{
        load(getFile("pets/schema.gql"));
        String template = Files.readLines(getFile("pets/template.gql"), StandardCharsets.UTF_8).stream().collect(joining("\n"));
        migrate(template, getFile("pets/quotes/emptyquotes.csv"));

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
        load(getFile("single-file/schema.gql"));
        assertNotNull(graph.getEntityType("make"));

        String template = Files.readLines(getFile("single-file/template.gql"), StandardCharsets.UTF_8).stream().collect(joining("\n"));
        migrate(template, getFile("single-file/data/cars.csv"));

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
        load(getFile("multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTypeTemplate = "insert $x isa pokemon-type has type-id <id>-type has description <identifier>;";
        String templated = csvMigrator.migrate(pokemonTypeTemplate, getFile("multi-file/data/types.csv"))
                .map(InsertQuery::toString)
                .collect(joining("\n"));

        System.out.println(templated);

        String expected = "id \"17-type\"";
        assertTrue(templated.contains(expected));
    }

    private void assertRelationExists(RelationType rel, Entity entity1, Entity entity2){
        assertTrue(rel.instances().stream().anyMatch(r ->
                r.rolePlayers().values().contains(entity1) && r.rolePlayers().values().contains(entity2)));
    }

    private File getFile(String fileName){
        return new File(CSVMigratorTest.class.getClassLoader().getResource(fileName).getPath());
    }

    // common class
    private void migrate(String template, File file){
        migrator.migrate(template, file);
    }

    private void load(File ontology) {
        try {
            Graql.withGraph(graph)
                    .parse(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException|MindmapsValidationException e){
            throw new RuntimeException(e);
        }
    }
}
