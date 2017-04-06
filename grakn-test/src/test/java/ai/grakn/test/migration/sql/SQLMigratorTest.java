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

package ai.grakn.test.migration.sql;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Resource;
import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.sql.SQLMigrator;
import ai.grakn.test.EngineContext;
import ai.grakn.test.migration.MigratorTestUtils;
import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.SQLException;

import static ai.grakn.test.migration.MigratorTestUtils.assertPetGraphCorrect;
import static ai.grakn.test.migration.MigratorTestUtils.assertPokemonGraphCorrect;
import static ai.grakn.test.migration.sql.SQLMigratorTestUtils.setupExample;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class SQLMigratorTest {

    private Migrator migrator;
    private GraknSession factory;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void setup(){
        factory = engine.factoryWithNewKeyspace();
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        graph.close();
        migrator = Migrator.to(Grakn.DEFAULT_URI, graph.getKeyspace());
    }

    @Test
    public void singleTableTest() throws SQLException {
        String template = MigratorTestUtils.getFileAsString("sql", "pets/template.gql");
        String query = "SELECT * FROM pet";

        try(Connection connection = setupExample(factory, "pets")){
            migrator.load(template, new SQLMigrator(query, connection).convert());

            GraknGraph graph = factory.open(GraknTxType.WRITE);
            assertPetGraphCorrect(graph);
        }
    }

    @Test
    public void multipleTableTest() throws SQLException {
        try(Connection connection = setupExample(factory, "pokemon")){
            String query = "SELECT * FROM type";
            String template =  "" +
                    "insert $x isa pokemon-type          " +
                    "   has type-id <ID>                 " +
                    "   has description <IDENTIFIER>;    ";

            migrator.load(template, new SQLMigrator(query, connection).convert());

            query = "SELECT * FROM pokemon";
            template = "" +
                    "insert $x isa pokemon                                         \n" +
                    "    has description <IDENTIFIER>                              \n" +
                    "    has pokedex-no <ID>                                       \n" +
                    "    has height <HEIGHT>                                       \n" +
                    "    has weight <WEIGHT>;                                      \n";

            migrator.load(template, new SQLMigrator(query, connection).convert());

            query = "SELECT * from pokemon";
            template = "" +
                    "match " +
                    "   $type isa pokemon-type; $type has type-id <TYPE1> if(<TYPE2> != null) do {or $type has type-id <TYPE2>};" +
                    "   $pokemon isa pokemon has description <IDENTIFIER> ;" +
                    "insert (pokemon-with-type: $pokemon, type-of-pokemon: $type) isa has-type;";

            migrator.load(template, new SQLMigrator(query, connection).convert());

            GraknGraph graph = factory.open(GraknTxType.WRITE);
            assertPokemonGraphCorrect(graph);
        }
    }

    @Test
    public void migrateOverJoinTest() throws SQLException {
        try(Connection connection = setupExample(factory, "pokemon")){

            String query = "SELECT * FROM type";
            String template =  "" +
                    "insert $x isa pokemon-type          " +
                    "   has type-id <ID>                 " +
                    "   has description <IDENTIFIER>;    ";

            migrator.load(template, new SQLMigrator(query, connection).convert());


            query = "SELECT * FROM pokemon";
            template = "" +
                    "insert $x isa pokemon               \n" +
                    "    has description <IDENTIFIER>    \n" +
                    "    has pokedex-no <ID>             \n" +
                    "    has height <HEIGHT>             \n" +
                    "    has weight <WEIGHT>;            \n";

            migrator.load(template, new SQLMigrator(query, connection).convert());

            query = "SELECT pokemon.identifier AS species, type.identifier AS type " +
                    "FROM pokemon, type WHERE pokemon.type1=type.id OR pokemon.type2=type.id";

            template = "" +
                    "match $type isa pokemon-type has description <TYPE>; \n" +
                    "      $pokemon isa pokemon has description   <SPECIES>; \n" +
                    "insert (pokemon-with-type: $pokemon, type-of-pokemon: $type) isa has-type;";

            migrator.load(template, new SQLMigrator(query, connection).convert());

            GraknGraph graph = factory.open(GraknTxType.WRITE);
            assertPokemonGraphCorrect(graph);
        }
    }

    @Test
    public void migrateWithSQLFunctionTest() throws SQLException {
        try(Connection connection = setupExample(factory, "pets")){
            String template = "insert $x isa count val <COUNT>;";
            String query = "SELECT count(*) AS count FROM pet";

            migrator.load(template, new SQLMigrator(query, connection).convert());

            GraknGraph graph = factory.open(GraknTxType.WRITE);
            Resource<Long> count = graph.getResourcesByValue(9L).iterator().next();
            assertNotNull(count);
            assertEquals(count.type(), graph.getResourceType("count"));
        }
    }

    @Test
    public void incorrectSQLStatementTest() throws SQLException {
        exception.expect(DataAccessException.class);

        try(Connection connection = setupExample(factory, "pokemon")) {

            String query = "SELECT * FROM nonexistant";
            String template = "" +
                    "insert $x isa pokemon-type          " +
                    "   has type-id <ID>                 " +
                    "   has description <IDENTIFIER>;    ";

            migrator.load(template, new SQLMigrator(query, connection).convert());
        }
    }
}