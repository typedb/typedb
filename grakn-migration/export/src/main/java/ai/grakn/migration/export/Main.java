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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.migration.base.io.MigrationCLI;

import java.util.Optional;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static ai.grakn.migration.base.io.MigrationCLI.initiateShutdown;
import static ai.grakn.migration.base.io.MigrationCLI.writeToSout;

/**
 * Export data from a Grakn graph to Graql statements - prints to System.out
 * @author alexandraorth
 */
public class Main {

    public static void main(String[] args){
        MigrationCLI.init(args, GraphWriterOptions::new).stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(Main::runExport);
    }

    public static void runExport(GraphWriterOptions options) {
        if(!options.exportOntology() && !options.exportData()) {
            writeToSout("Missing arguments -ontology and/or -data");
            die("");
        }

        writeToSout("Writing graph " + options.getKeyspace() + " using Grakn Engine " +
                options.getUri() + " to System.out");

        try(GraknGraph graph = Grakn.factory(options.getUri(), options.getKeyspace()).getGraph()) {
            GraphWriter graphWriter = new GraphWriter(graph);

            if (options.exportOntology()) {
                writeToSout(graphWriter.dumpOntology());
            }

            if (options.exportData()) {
                writeToSout(graphWriter.dumpData());
            }
        }
        initiateShutdown();
    }
}
