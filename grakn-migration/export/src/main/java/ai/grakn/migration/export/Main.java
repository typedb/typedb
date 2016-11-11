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
package ai.grakn.migration.export;

import ai.grakn.GraknGraph;
import ai.grakn.migration.base.io.MigrationCLI;
import org.apache.commons.cli.Options;

/**
 * Export data from Grakn. If no file is provided, it will dump graph content to standard out.
 */
public class Main {

    private static Options options = new Options();
    static {
        options.addOption("ontology", false, "export ontology");
        options.addOption("data", false, "export data");
    }

    public static void main(String[] args){
        MigrationCLI.create(args, options).ifPresent(Main::runExport);
    }

    public static void runExport(MigrationCLI cli){
        if(!cli.hasOption("ontology") && !cli.hasOption("data")) {
            cli.writeToSout("Missing arguments -ontology and/or -data");
            cli.die("");
        }

        cli.writeToSout("Writing graph " + cli.getKeyspace() + " using Grakn Engine " +
                cli.getEngineURI() + " to System.out");

        GraknGraph graph = cli.getGraph();
        GraphWriter graphWriter = new GraphWriter(graph);

        if(cli.hasOption("ontology")){
            cli.writeToSout(graphWriter.dumpOntology());
        }

        if(cli.hasOption("data")){
            cli.writeToSout(graphWriter.dumpOntology());
        }

        cli.initiateShutdown();
    }
}
