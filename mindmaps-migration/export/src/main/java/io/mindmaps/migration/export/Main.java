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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

import static io.mindmaps.migration.base.io.MigrationCLI.die;

public class Main {

    static {
        MigrationCLI.addOption("f", "file", true, "output file");
        MigrationCLI.addOption("o", "ontology", false, "export ontology");
        MigrationCLI.addOption("d", "data", false, "export data");
    }

    public static void main(String[] args){

        MigrationCLI interpreter = new MigrationCLI(args);

        String outputFile = interpreter.getOption("f");

        System.out.println("Writing graph " + interpreter.getKeyspace() + " using MM Engine " +
                interpreter.getEngineURI() + " to " + (outputFile == null ? "System.out" : outputFile));

        MindmapsGraph graph = interpreter.getGraph();
        GraphWriter graphWriter = new GraphWriter(graph);

        StringBuilder builder = new StringBuilder();
        if(interpreter.hasOption("o")){
            builder.append(graphWriter.dumpOntology());
        }

        if(interpreter.hasOption("d")){
           builder.append(graphWriter.dumpData());
        }

        Writer writer = null;
        try {
            writer = outputFile != null ? new FileWriter(outputFile) : new PrintWriter(System.out);

            // If there is no fileWriter, use a printWriter
            writer.write(builder.toString());
            writer.flush();
        } catch (IOException e){
            die("Problem writing to file " + outputFile);
        } finally {
            if(outputFile != null && writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    die("Problem closing output stream.");
                }
            }
            graph.close();
        }
    }
}
