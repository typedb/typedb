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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.ResourceType;
import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.test.EngineContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static ai.grakn.test.migration.MigratorTestUtils.assertPetGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.assertPokemonGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.getFileAsString;
import static ai.grakn.test.migration.MigratorTestUtils.load;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CSVMigratorTest {

    private GraknSession factory;
    private Migrator migrator;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setup() {
        factory = engine.factoryWithNewKeyspace();
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        migrator = Migrator.to(Grakn.DEFAULT_URI, graph.getKeyspace());
        graph.close();
    }

    @Test
    public void multiFileMigrateGraphPersistenceTest(){
        load(factory, getFile("csv", "multi-file/schema.gql"));

        String pokemonTemplate = "" +
                "insert $x isa pokemon                      " +
                "    has description <identifier>  \n" +
                "    has pokedex-no <id>           \n" +
                "    has height @int(<height>)       \n" +
                "    has weight @int(<weight>);        ";

        String pokemonTypeTemplate = "               " +
                "insert $x isa pokemon-type                 " +
                "   has type-id <id>                 " +
                "   has description <identifier>;    ";

        String edgeTemplate = "" +
                "match                                            " +
                "   $pokemon has pokedex-no <pokemon_id>        ; " +
                "   $type has type-id <type_id>                 ; " +
                "insert (pokemon-with-type: $pokemon, type-of-pokemon: $type) isa has-type;";

        declareAndLoad(pokemonTemplate,  "multi-file/data/pokemon.csv");
        declareAndLoad(pokemonTypeTemplate,  "multi-file/data/types.csv");
        declareAndLoad(edgeTemplate,  "multi-file/data/edges.csv");

        GraknGraph graph = factory.open(GraknTxType.WRITE);//Re Open Transaction
        assertPokemonGraphCorrect(graph);
    }

    @Test
    public void quotesWithoutContentTest() throws IOException {
        load(factory, getFile("csv", "pets/schema.gql"));
        String template = getFileAsString("csv", "pets/template.gql");

        declareAndLoad(template,  "pets/data/pets.quotes");

        GraknGraph graph = factory.open(GraknTxType.WRITE);//Re Open Transaction
        assertPetGraphCorrect(graph);
    }

    @Test
    public void testMissingDataDoesNotThrowError() {
        load(factory, getFile("csv", "pets/schema.gql"));
        String template = getFileAsString("csv", "pets/template.gql");

        try(CSVMigrator m = new CSVMigrator(getFile("csv", "pets/data/pets.empty"))) {
            migrator.load(template, m.setNullString("").convert());
        }

        GraknGraph graph = factory.open(GraknTxType.WRITE);//Re Open Transaction

        Collection<Entity> pets = graph.getEntityType("pet").instances();
        assertEquals(1, pets.size());

        Collection<Entity> cats = graph.getEntityType("cat").instances();
        assertEquals(1, cats.size());

        ResourceType<String> name = graph.getResourceType("name");
        ResourceType<String> death = graph.getResourceType("death");

        Entity fluffy = name.getResource("Fluffy").ownerInstances().iterator().next().asEntity();
        assertEquals(1, fluffy.resources(death).size());
    }

    @Test
    public void parsedLineIsEmpty_MigratorSkipsThatLine(){
        load(factory, getFile("csv", "pets/schema.gql"));

        // Only insert Puffball
        String template = "if (<name> != \"Puffball\") do { insert $x isa pet; }";
        declareAndLoad(template, "pets/data/pets.quotes");

        GraknGraph graph = factory.open(GraknTxType.WRITE);//Re Open Transaction
        assertEquals(1, graph.getEntityType("pet").instances().size());
    }

    @Ignore //Ignored because this feature is not yet supported
    @Test
    public void multipleEntitiesInOneFileTest() throws IOException {
        load(factory, getFile("csv", "single-file/schema.gql"));

        GraknGraph graph = factory.open(GraknTxType.WRITE);//Re Open Transaction
        assertNotNull(graph.getEntityType("make"));

        String template = getFileAsString("csv", "single-file/template.gql");
        declareAndLoad(template, "single-file/data/cars.csv");

        // test
        Collection<Entity> makes = graph.getEntityType("make").instances();
        assertEquals(3, makes.size());

        Collection<Entity> models = graph.getEntityType("model").instances();
        assertEquals(4, models.size());

        // test empty value not created
        ResourceType description = graph.getResourceType("description");

        Entity venture = graph.getConcept(ConceptId.of("Venture"));
        assertEquals(1, venture.resources(description).size());

        Entity ventureLarge = graph.getConcept(ConceptId.of("Venture Large"));
        assertEquals(0, ventureLarge.resources(description).size());
    }

    private void declareAndLoad(String template, String file){
        try(CSVMigrator m = new CSVMigrator(getFile("csv", file))) {
            migrator.load(template, m.convert());
        }
    }
}
