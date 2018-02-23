/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.bootup;

import ai.grakn.graql.GraqlShell;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.migration.export.Main;
import ai.grakn.migration.json.JsonMigrator;
import ai.grakn.migration.sql.SQLMigrator;
import ai.grakn.migration.xml.XmlMigrator;
import ai.grakn.util.GraknVersion;

import java.util.Arrays;

/**
 *
 * @author Michele Orsi
 */
public class Graql {

    /**
     *
     * Invocation from bash script 'graql'
     *
     * @param args
     */
    public static void main(String[] args) {
        new Graql().run(args);
    }

    public void run(String[] args) {
        String context = args.length > 0 ? args[0] : "";

        switch (context) {
            case "console":
                GraqlShell.main(valuesFrom(args, 1));
                break;
            case "migrate":
                migrate(valuesFrom(args, 1));
                break;
            case "version":
                version();
                break;
            default: help();
        }

    }

    private void migrate(String[] args) {
        String option = args.length > 0 ? args[0] : "";

        switch (option) {
            case "csv":
                CSVMigrator.main(valuesFrom(args, 1));
                break;
            case "json":
                JsonMigrator.main(valuesFrom(args, 1));
                break;
            case "owl":
                System.out.println("Owl migration not supported anymore");
                break;
            case "export":
                Main.main(valuesFrom(args, 1));
                break;
            case "sql":
                SQLMigrator.main(valuesFrom(args, 1));
                break;
            case "xml":
                XmlMigrator.main(valuesFrom(args, 1));
                break;
            default:
                help();
        }
    }

    private String[] valuesFrom(String[] args, int index) {
        return Arrays.copyOfRange(args, index, args.length);
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
        System.out.println(GraknVersion.VERSION);
    }

}
