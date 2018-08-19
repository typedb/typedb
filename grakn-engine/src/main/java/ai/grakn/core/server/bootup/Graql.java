/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.core.server.bootup;

import ai.grakn.graql.shell.GraqlConsole;
import ai.grakn.graql.shell.GraqlShellOptions;
import ai.grakn.graql.shell.GraqlShellOptionsFactory;
import ai.grakn.migration.csv.CSVMigrator;
import ai.grakn.migration.export.Main;
import ai.grakn.migration.json.JsonMigrator;
import ai.grakn.migration.sql.SQLMigrator;
import ai.grakn.migration.xml.XmlMigrator;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import com.google.common.base.StandardSystemProperty;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.Arrays;

/**
 * The main class of the 'graql' command. This class is not a class responsible
 * for booting up the real command, but rather the command itself.
 *
 * Please keep the class name "Graql" as it is what will be displayed to the user.
 *
 * @author Michele Orsi
 */
public class Graql {

    private final GraqlShellOptionsFactory graqlShellOptionsFactory;
    private static final String HISTORY_FILENAME = StandardSystemProperty.USER_HOME.value() + "/.graql-history";


    public Graql(GraqlShellOptionsFactory graqlShellOptionsFactory) {
        this.graqlShellOptionsFactory = graqlShellOptionsFactory;
    }

    /**
     *
     * Invocation from bash script 'graql'
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        GraqlShellOptionsFactory graqlShellOptionsFactory = GraqlShellOptions::create;

        new Graql(graqlShellOptionsFactory).run(args);
    }

    public void run(String[] args) throws IOException, InterruptedException {
        String context = args.length > 0 ? args[0] : "";

        switch (context) {
            case "console":
                GraqlShellOptions options;

                try {
                    options = graqlShellOptionsFactory.createGraqlShellOptions(valuesFrom(args, 1));
                } catch (ParseException e) {
                    System.err.println(e.getMessage());
                    return;
                }

                GraqlConsole.start(options, HISTORY_FILENAME, System.out, System.err);
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
                System.err.println(ErrorMessage.OWL_NOT_SUPPORTED.getMessage());
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
