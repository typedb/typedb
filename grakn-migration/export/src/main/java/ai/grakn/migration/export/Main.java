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
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.migration.base.MigrationCLI;

import java.util.Optional;

/**
 * Export data from a Grakn graph to Graql statements - prints to System.out
 * @author alexandraorth
 */
public class Main {

    public static void main(String[] args){
        try{
            MigrationCLI.init(args, GraphWriterOptions::new).stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(Main::runExport);
        } catch (Throwable throwable){
            System.err.println(throwable.getMessage());
        }
    }

    private static void runExport(GraphWriterOptions options) {
        if(!options.exportOntology() && !options.exportData()) {
            throw new IllegalArgumentException("Missing arguments -ontology and/or -data");
        }

        try(GraknTx graph = Grakn.session(options.getUri(), options.getKeyspace()).open(GraknTxType.READ)) {
            GraphWriter graphWriter = new GraphWriter(graph);

            if (options.exportOntology()) {
                System.out.println(graphWriter.dumpOntology());
            }

            if (options.exportData()) {
                System.out.println(graphWriter.dumpData());
            }
        }
    }
}
