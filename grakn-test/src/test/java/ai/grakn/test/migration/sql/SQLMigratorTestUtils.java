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

import ai.grakn.GraknSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static ai.grakn.test.migration.MigratorTestUtils.getFile;
import static ai.grakn.test.migration.MigratorTestUtils.getFileAsString;
import static ai.grakn.test.migration.MigratorTestUtils.load;

public class SQLMigratorTestUtils {

    public static final String USER = "test";
    public static final String PASS = "";
    public static final String DRIVER = "org.h2.Driver";
    public static final String URL = "jdbc:h2:~/test;";

    public static Connection setupExample(GraknSession factory, String example) throws SQLException {
        load(factory, getFile("sql", example + "/schema.gql"));

        // read schema and data from file
        String schema = getFileAsString("sql", example + "/create-db.sql");
        String data = getFileAsString("sql", example + "/insert-data.sql");

        Connection connection;
        try {
            Class.forName(DRIVER).newInstance();
            connection = DriverManager.getConnection(URL, USER, PASS);
        }
        catch (SQLException|ClassNotFoundException|InstantiationException|IllegalAccessException e){
            throw new RuntimeException(e);
        }

        // attempt to clear DB
        try { connection.prepareStatement("DROP ALL OBJECTS").execute(); }
        catch (SQLException e){
            System.out.println("Was not able to drop all objects due to DB type");
        }

        // load schema and data into db
        connection.prepareStatement(schema).execute();
        connection.prepareStatement(data).execute();

        return connection;
    }

}
