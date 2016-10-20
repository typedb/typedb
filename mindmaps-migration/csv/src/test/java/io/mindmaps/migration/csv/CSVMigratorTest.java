///*
// * MindmapsDB - A Distributed Semantic Database
// * Copyright (C) 2016  Mindmaps Research Ltd
// *
// * MindmapsDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published kemby
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * MindmapsDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
// */
//
//package io.mindmaps.migration.csv;
//
//import com.google.common.io.Files;
//import io.mindmaps.MindmapsGraph;
//import io.mindmaps.concept.*;
//import io.mindmaps.engine.MindmapsEngineServer;
//import io.mindmaps.engine.loader.BlockingLoader;
//import io.mindmaps.engine.util.ConfigProperties;
//import io.mindmaps.exception.MindmapsValidationException;
//import io.mindmaps.factory.GraphFactory;
//import io.mindmaps.graql.Graql;
//import org.junit.*;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.util.Collection;
//
//import static io.mindmaps.migration.base.util.MigratorTestUtil.load;
//import static java.util.stream.Collectors.joining;
//import static junit.framework.TestCase.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
//
//public class CSVMigratorTest {
//
//    private MindmapsGraph graph;
//    private CSVMigrator migrator;
//
//    @BeforeClass
//    public static void start(){
//        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
//        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");
//
//        MindmapsEngineServer.start();
//    }
//
//    @AfterClass
//    public static void stop(){
//        MindmapsEngineServer.stop();
//    }
//
//    @Before
//    public void setup(){
//        String GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
//
//        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
//        BlockingLoader loader = new BlockingLoader(GRAPH_NAME);
//        loader.setExecutorSize(1);
//
//        migrator = new CSVMigrator(loader);
//    }
//
//    @After
//    public void shutdown(){
//        graph.clear();
//    }
//
//    @Test
//    public void multiFileMigrateGraqlStringTest(){
//
//    }
//
//    @Test
//    public void multiFileMigrateGraphPersistenceTest(){
//        load(graph, get("multi-file/schema.gql"));
//        assertNotNull(graph.getEntityType("pokemon"));
//
//        String pokemonTemplate = "" +
//                "$x isa pokemon id <id>-pokemon \n" +
//                "    has description <identifier>\n" +
//                "    has pokedex-no @int(id)\n" +
//                "    has height @int(height)\n" +
//                "    has weight @int(weight);";
//
//        String pokemonTypeTemplate = "$x isa pokemon-type id <id>-type has description <identifier>;";
//
//        String edgeTemplate = "(pokemon-with-type: <pokemon_id>-pokemon, type-of-pokemon: <type_id>-type) isa has-type;";
//
//        migrator.migrate(pokemonTemplate, get("multi-file/data/pokemon.csv"));
//        migrator.migrate(pokemonTypeTemplate, get("multi-file/data/types.csv"));
//        migrator.migrate(edgeTemplate, get("multi-file/data/edges.csv"));
//
//        Collection<Entity> pokemon = graph.getEntityType("pokemon").instances();
//        assertEquals(151, pokemon.size());
//
//        Entity grass = graph.getEntity("12-type");
//        Entity poison = graph.getEntity("4-type");
//        Entity bulbasaur = graph.getEntity("1-pokemon");
//        RelationType relation = graph.getRelationType("has-type");
//
//        assertRelationExists(relation, bulbasaur, grass);
//        assertRelationExists(relation, bulbasaur, poison);
//    }
//
//    @Test
//    public void multipleEntitiesInOneFileTest(){
//        load(graph, get("single-file/schema.gql"));
//        assertNotNull(graph.getEntityType("make"));
//
//        String template = "" +
//                "$x isa make id <Make>;\n" +
//                "$y isa model id <Model>\n" +
//                "    if (ne Year null) do {has year <Year> }\n " +
//                "    if (ne Description null) do { has description <Description> }\n" +
//                "    if (ne Price null) do { has price @double(Price) ;\n" +
//                "(make-of-car: $x, model-of-car: $y) isa make-and-model;";
//
//        migrator.migrate(template, get("single-file/data/cars.csv"));
//
//        // test
//        Collection<Entity> makes = graph.getEntityType("make").instances();
//        assertEquals(3, makes.size());
//
//        Collection<Entity> models = graph.getEntityType("model").instances();
//        assertEquals(4, models.size());
//
//        // test empty value not created
//        ResourceType description = graph.getResourceType("description");
//
//        Entity venture = graph.getEntity("Venture");
//        assertEquals(1, venture.resources(description).size());
//
//        Entity ventureLarge = graph.getEntity("Venture Large");
//        assertEquals(0, ventureLarge.resources(description).size());
//    }
//
//    @Test
//    public void testMigrateAsStringMethod(){
//        load(graph, get("multi-file/schema.gql"));
//        assertNotNull(graph.getEntityType("pokemon"));
//
//        String pokemonTypeTemplate = "$x isa pokemon-type id <id>-type has description <identifier>;";
//        String templated = migrator.graql(pokemonTypeTemplate, get("multi-file/data/types.csv"));
//
//        String expected = "insert  $x0 isa pokemon-type id \"1-type\" has description \"normal\"; $x1 isa pokemon-type id \"2-type\" has description \"fighting\"; $x2 isa pokemon-type id \"3-type\" has description \"flying\"; $x3 isa pokemon-type id \"4-type\" has description \"poison\"; $x4 isa pokemon-type id \"5-type\" has description \"ground\";\n" +
//                "insert  $x0 isa pokemon-type id \"6-type\" has description \"rock\"; $x1 isa pokemon-type id \"7-type\" has description \"bug\"; $x2 isa pokemon-type id \"8-type\" has description \"ghost\"; $x3 isa pokemon-type id \"9-type\" has description \"steel\"; $x4 isa pokemon-type id \"10-type\" has description \"fire\";\n" +
//                "insert  $x0 isa pokemon-type id \"11-type\" has description \"water\"; $x1 isa pokemon-type id \"12-type\" has description \"grass\"; $x2 isa pokemon-type id \"13-type\" has description \"electric\"; $x3 isa pokemon-type id \"14-type\" has description \"psychic\"; $x4 isa pokemon-type id \"15-type\" has description \"ice\";\n" +
//                "insert  $x0 isa pokemon-type id \"16-type\" has description \"dragon\"; $x1 isa pokemon-type id \"17-type\" has description \"dark\"; $x2 isa pokemon-type id \"18-type\" has description \"fairy\"; $x3 isa pokemon-type id \"10001-type\" has description \"unknown\"; $x4 isa pokemon-type id \"10002-type\" has description \"shadow\";";
//
//        assertEquals(expected, templated);
//    }
//
//    @Test
//    public void icijTest() {
//
////        schemaMigrator.configure("entity", parser("icij/data/Entities.csv")).migrator.migrate(loader);
//
//        // test a entity
////        Entity thing = Graql.withGraph(graph).match(var("x").isa("entity")).iterator().next().get("x").asEntity();
////        assertNotNull(thing);
////        assertResourceRelationExists("name-resource", thing);
////        assertResourceRelationExists("country_codes-resource", thing);
//
////        schemaMigrator.configure("address", parser("icij/data/Addresses.csv")).migrator.migrate(loader);
//
//        // test an address
////        thing = Graql.withGraph(graph).match(var("x").isa("address")).iterator().next().get("x").asEntity();
////        assertNotNull(thing);
////        assertResourceRelationExists("valid_until-resource", thing);
////        assertResourceRelationExists("countries-resource", thing);
//
////        schemaMigrator.configure("officer", parser("icij/data/Officers.csv")).migrator.migrate(loader);
//
////        // test an officer
////        thing = Graql.withGraph(graph).match(var("x").isa("officer")).iterator().next().get("x").asEntity();
////        assertNotNull(thing);
////        assertResourceRelationExists("valid_until-resource", thing);
////        assertResourceRelationExists("country_codes-resource", thing);
//
////        schemaMigrator.configure("intermediary", parser("icij/data/Intermediaries.csv")).migrator.migrate(loader);
//
//        // test an intermediary
////        thing = Graql.withGraph(graph).match(var("x").isa("intermediary")).iterator().next().get("x").asEntity();
////        assertNotNull(thing);
////        assertResourceRelationExists("countries-resource", thing);
////        assertResourceRelationExists("status-resource", thing);
//    }
//
//    private File get(String fileName){
//        return new File(CSVMigratorTest.class.getClassLoader().getResource(fileName).getPath());
//    }
//}
