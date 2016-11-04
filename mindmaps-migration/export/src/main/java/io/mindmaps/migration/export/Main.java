/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
package io.mindmaps.migration.export;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.migration.base.io.MigrationCLI;
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

        MigrationCLI cli = new MigrationCLI(args, options);

        String outputFile = cli.getOption("destination");

        System.out.println("Writing graph " + cli.getKeyspace() + " using MM Engine " +
                cli.getEngineURI() + " to " + (outputFile == null ? "System.out" : outputFile));

        MindmapsGraph graph = cli.getGraph();
        GraphWriter graphWriter = new GraphWriter(graph);

        if(cli.hasOption("ontology")){
            cli.writeToSout(graphWriter.dumpOntology());
        }

        if(cli.hasOption("data")){
            cli.writeToSout(graphWriter.dumpOntology());
        }

        System.exit(0);
    }
}
