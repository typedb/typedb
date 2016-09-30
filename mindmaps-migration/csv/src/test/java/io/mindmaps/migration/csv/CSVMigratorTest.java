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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.io.Files;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Entity;
import io.mindmaps.engine.controller.CommitLogController;
import io.mindmaps.engine.controller.GraphFactoryController;
import io.mindmaps.engine.controller.TransactionController;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.mindmaps.graql.Graql.var;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CSVMigratorTest {

    private String GRAPH_NAME;

    private MindmapsGraph graph;
    private BlockingLoader loader;

    private static CSVMigrator migrator;

    @BeforeClass
    public static void start(){
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);

        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY,ConfigProperties.TEST_CONFIG_FILE);
        System.setProperty(ConfigProperties.CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir")+"/../");

        new TransactionController();
        new CommitLogController();
        new GraphFactoryController();
    }

    @Before
    public void setup(){
        GRAPH_NAME = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        graph = GraphFactory.getInstance().getGraphBatchLoading(GRAPH_NAME);
        loader = new BlockingLoader(GRAPH_NAME);

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
                "    has pokedex-no @noescp{<id>}\n" +
                "    has height @noescp{<height>}\n" +
                "    has weight @noescp{<weight>};";

        String pokemonTypeTemplate = "$x isa pokemon-type id <id>-type has description <identifier>;";

        String edgeTemplate = "(pokemon-with-type: <pokemon_id>, type-of-pokemon: <type_id>) isa has-type;";

        migrate(pokemonTemplate, get("multi-file/data/pokemon.csv"));
        migrate(pokemonTypeTemplate, get("multi-file/data/types.csv"));
        migrate(edgeTemplate, get("multi-file/data/edges.csv"));
    }

    @Test
    public void multiFileOutOfOrderTest(){
        load(get("multi-file/schema.gql"));

        assertNotNull(graph.getEntityType("pokemon"));

        String pokemonTemplate = "" +
                "$x isa pokemon\n" +
                "    has pokedex-no <id>\n" +
                "    has name <identifier>\n" +
                "    has height @noescp{ <height>}\n" +
                "    has weight @noescp{ <weight>};";

        String pokemonTypeTemplate = "$x isa pokemon-type id <identifier>;";

        String edgeTemplate = "(pokemon-with-type: <pokemon_id>, type-of-pokemon: <type_id>) isa has-type;";

        // migrating edges before types
        migrate(pokemonTemplate, get("multi-file/data/pokemon.csv"));
        migrate(edgeTemplate, get("multi-file/data/edges.csv"));
        migrate(pokemonTypeTemplate, get("multi-file/data/types.csv"));
    }

    @Test
    public void multipleEntitiesInOneFileTest(){

    }

    @Test
    public void icijTest() {

//        schemaMigrator.configure("entity", parser("icij/data/Entities.csv")).migrate(loader);

        // test a entity
        Entity thing = Graql.withGraph(graph).match(var("x").isa("entity")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("name-resource", thing);
        assertResourceRelationExists("country_codes-resource", thing);

//        schemaMigrator.configure("address", parser("icij/data/Addresses.csv")).migrate(loader);

        // test an address
        thing = Graql.withGraph(graph).match(var("x").isa("address")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("valid_until-resource", thing);
        assertResourceRelationExists("countries-resource", thing);

//        schemaMigrator.configure("officer", parser("icij/data/Officers.csv")).migrate(loader);

//        // test an officer
        thing = Graql.withGraph(graph).match(var("x").isa("officer")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("valid_until-resource", thing);
        assertResourceRelationExists("country_codes-resource", thing);

//        schemaMigrator.configure("intermediary", parser("icij/data/Intermediaries.csv")).migrate(loader);

        // test an intermediary
        thing = Graql.withGraph(graph).match(var("x").isa("intermediary")).iterator().next().get("x").asEntity();
        assertNotNull(thing);
        assertResourceRelationExists("countries-resource", thing);
        assertResourceRelationExists("status-resource", thing);
    }

    private void assertResourceRelationExists(String type, Entity owner){
        assertTrue(owner.resources().stream().anyMatch(resource ->
                resource.type().getId().equals(type)));
    }

    private void migrate(String template, File file){
        migrator.migrate(template, file);
    }

    private File get(String fileName){
        return new File(CSVMigratorTest.class.getClassLoader().getResource(fileName).getPath());
    }

    private void load(File ontology) {
        try {
            Graql.withGraph(graph)
                    .parseInsert(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException|MindmapsValidationException e){
            throw new RuntimeException(e);
        }
    }
}
