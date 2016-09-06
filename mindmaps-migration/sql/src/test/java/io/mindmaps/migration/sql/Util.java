package io.mindmaps.migration.sql;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class Util {

    public static String readSql(String name) {
        try {
            URL dataUrl = SQLSchemaMigratorTest.class.getClassLoader().getResource(name);
            assert dataUrl != null;
            return Files.readAllLines(Paths.get(dataUrl.toURI())).stream()
                    .filter(line -> !line.startsWith("--"))
                    .collect(Collectors.joining());
        }
        catch (URISyntaxException |IOException e){
            e.printStackTrace();
        }
        return null;
    }

    public static String readSqlSchema(String name){
        return readSql(name + "/create-db.sql");
    }

    public static String readSqlData(String name){
        return readSql(name + "/insert-data.sql");
    }

    public static Connection setupExample(String example) throws SQLException {

        // read schema and data from file
        String schema = readSqlSchema(example);
        String data = readSqlData(example);

        String user = "test";
        String pass = "";
        String driver = "org.h2.Driver";
        String url = "jdbc:h2:~/test;";

        Connection connection;
        try {
            Class.forName(driver).newInstance();
            connection = DriverManager.getConnection(url, user, pass);
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
