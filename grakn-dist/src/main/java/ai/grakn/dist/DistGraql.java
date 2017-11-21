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

package ai.grakn.dist;

import ai.grakn.GraknSystemProperty;
import ai.grakn.graql.GraqlShell;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.migration.json.JsonMigrator;
import ai.grakn.migration.sql.SQLMigrator;
import ai.grakn.migration.xml.XmlMigrator;

import java.util.Arrays;

/**
 *
 * @author Michele Orsi
 */
public class DistGraql {

    private final String homePath;
    private final ProcessHandler processHandler;
    private String classpath;

    public DistGraql(String homeStatic, ProcessHandler processHandler) {

        this.homePath = homeStatic;
        this.processHandler = processHandler;
    }

    /**
     * In order to run this method you should have 'grakn.dir' set
     *
     * @param args
     */
    public static void main(String[] args) {
        String home = GraknSystemProperty.CURRENT_DIRECTORY.value();

        if(home==null) {
            System.out.println("Problem with bash script: cannot run Grakn");
            return;
        }

        DistGraql application = new DistGraql(home, new ProcessHandler());
        try {
            application.run(args);
        } catch (RuntimeException ex) {
            System.out.println(ex.getMessage());
        }

    }

    private void run(String[] args) {
        classpath = processHandler.getClassPathFrom(homePath);

        String arg0 = args.length > 0 ? args[0] : "";

        switch (arg0) {
            case "console":
                GraqlShell.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "migrate":
                migrate(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "version":
                version();
                break;
            default: help();
        }

    }

    private void migrate(String[] args) {
        String arg0 = args.length > 0 ? args[0] : "";

        switch (arg0) {
            case "csv":
                CSVMigrator.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "json":
                JsonMigrator.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "owl":
                System.out.println("Owm migration not supported anymore");
                break;
            case "export":
                ai.grakn.migration.export.Main.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "sql":
                SQLMigrator.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            case "xml":
                XmlMigrator.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            default:
                XmlMigrator.main(new String[]{"-h"});
        }
    }

    private void help() {
        System.out.println("Usage: graql COMMAND\n" +
                "\n" +
                "COMMAND:\n" +
                "console  Start a REPL console for running Graql queries. Defaults to connecting to http://localhost\n" +
                "migrate  Run migration from a file\n" +
                "version  Print Grakn version\n" +
                "help     Print this message");

    }

    private void version() {
        GraqlShell.main(new String[]{"--v"});
    }

}
