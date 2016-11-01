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

package ai.grakn.migration.csv;

import com.google.common.io.Files;
import ai.grakn.GraknGraph;
import ai.grakn.concept.*;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.loader.BlockingLoader;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.Graql;
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

    private GraknGraph graph;
    private CSVMigrator migrator;

    @BeforeClass
    public static void start(){
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        GraknEngineServer.start();
    }

    @AfterClass
    public static void stop(){
        GraknEngineServer.stop();
    }

    @Before
    public void setup(){
        String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
        BlockingLoader loader = new BlockingLoader(GRAPH_NAME);
        loader.setExecutorSize(1);

        migrator = new CSVMigrator(loader);
    }

    @After
    public void shutdown(){
        graph.clear();
    }

    @Test
    public void multiFileMigrateGraqlStringTest(){

    }

    @Test
    public void multiFileMigrateGraphPersistenceTest(){
        load(get("multi-file/schema.gql"));
        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTemplate = "" +
                "$x isa pokemon id <id>-pokemon \n" +
                "    has description <identifier>\n" +
                "    has pokedex-no @int(id)\n" +
                "    has height @int(height)\n" +
                "    has weight @int(weight);";

        String pokemonTypeTemplate = "$x isa pokemon-type id <id>-type has description <identifier>;";

        String edgeTemplate = "(pokemon-with-type: <pokemon_id>-pokemon, type-of-pokemon: <type_id>-type) isa has-type;";

        migrate(pokemonTemplate, get("multi-file/data/pokemon.csv"));
        migrate(pokemonTypeTemplate, get("multi-file/data/types.csv"));
        migrate(edgeTemplate, get("multi-file/data/edges.csv"));

        Collection<Entity> pokemon = graph.getEntityType("pokemon").instances();
        assertEquals(151, pokemon.size());

        Entity grass = graph.getEntity("12-type");
        Entity poison = graph.getEntity("4-type");
        Entity bulbasaur = graph.getEntity("1-pokemon");
        RelationType relation = graph.getRelationType("has-type");

        assertRelationExists(relation, bulbasaur, grass);
        assertRelationExists(relation, bulbasaur, poison);
    }

    @Test
    public void multipleEntitiesInOneFileTest(){
        load(get("single-file/schema.gql"));
        assertNotNull(graph.getEntityType("make"));

        String template = "" +
                "$x isa make id <Make>;\n" +
                "$y isa model id <Model>\n" +
                "    if (ne Year null) do {has year <Year> }\n " +
                "    if (ne Description null) do { has description <Description> }\n" +
                "    if (ne Price null) do { has price @double(Price) ;\n" +
                "(make-of-car: $x, model-of-car: $y) isa make-and-model;";

        migrate(template, get("single-file/data/cars.csv"));

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
    public void icijTest() {

//        schemaMigrator.configure("entity", parser("icij/data/Entities.csv")).migrate(loader);

        // test a entity
//        Entity thing = Graql.withGraph(graph).match(var("x").isa("entity")).iterator().next().get("x").asEntity();
//        assertNotNull(thing);
//        assertResourceRelationExists("name-resource", thing);
//        assertResourceRelationExists("country_codes-resource", thing);

//        schemaMigrator.configure("address", parser("icij/data/Addresses.csv")).migrate(loader);

        // test an address
//        thing = Graql.withGraph(graph).match(var("x").isa("address")).iterator().next().get("x").asEntity();
//        assertNotNull(thing);
//        assertResourceRelationExists("valid_until-resource", thing);
//        assertResourceRelationExists("countries-resource", thing);

//        schemaMigrator.configure("officer", parser("icij/data/Officers.csv")).migrate(loader);

//        // test an officer
//        thing = Graql.withGraph(graph).match(var("x").isa("officer")).iterator().next().get("x").asEntity();
//        assertNotNull(thing);
//        assertResourceRelationExists("valid_until-resource", thing);
//        assertResourceRelationExists("country_codes-resource", thing);

//        schemaMigrator.configure("intermediary", parser("icij/data/Intermediaries.csv")).migrate(loader);

        // test an intermediary
//        thing = Graql.withGraph(graph).match(var("x").isa("intermediary")).iterator().next().get("x").asEntity();
//        assertNotNull(thing);
//        assertResourceRelationExists("countries-resource", thing);
//        assertResourceRelationExists("status-resource", thing);
    }

    private void assertResourceRelationExists(String type, Entity owner){
        assertTrue(owner.resources().stream().anyMatch(resource ->
                resource.type().getId().equals(type)));
    }

    private void assertRelationExists(RelationType rel, Entity entity1, Entity entity2){
        assertTrue(rel.instances().stream().anyMatch(r ->
                r.rolePlayers().values().contains(entity1) && r.rolePlayers().values().contains(entity2)));
    }

    private File get(String fileName){
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
        } catch (IOException|GraknValidationException e){
            throw new RuntimeException(e);
        }
    }
}
