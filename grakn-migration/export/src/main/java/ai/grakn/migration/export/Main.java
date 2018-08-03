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
package ai.grakn.migration.export;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.client.Grakn;
import ai.grakn.migration.base.MigrationCLI;

import java.util.Optional;

/**
 * Export data from a Grakn graph to Graql statements - prints to System.out
 * @author alexandraorth
 */
public class Main {

    public static void main(String[] args){
        try{
            MigrationCLI.init(args, KBWriterOptions::new).stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(Main::runExport);
        } catch (IllegalArgumentException e){
            System.err.println(e.getMessage());
        }
    }

    private static void runExport(KBWriterOptions options) {
        if(!options.exportSchema() && !options.exportData()) {
            throw new IllegalArgumentException("Missing arguments -schema and/or -data");
        }

        try(GraknTx tx = new Grakn(options.getUri()).session(options.getKeyspace()).transaction(GraknTxType.READ)) {
            KBWriter graphWriter = new KBWriter(tx);

            if (options.exportSchema()) {
                System.out.println(graphWriter.dumpSchema());
            }

            if (options.exportData()) {
                System.out.println(graphWriter.dumpData());
            }
        }
    }
}
