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

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Main {

    static void die(String errorMsg){
        throw new RuntimeException(errorMsg + "\nSyntax: ./migration.sh export {ontology, data} -file <output file> -graph <graph name> [-engine <Mindmaps engine URL>]");
    }

    public static void main(String[] args){

        String graphName = null;
        String outputFileName = null;
        String engineURL = null;
        boolean ontology = false;
        boolean data = false;

        for (int i = 0; i < args.length; i++) {
            if ("-file".equals(args[i]))
                outputFileName = args[++i];
            else if("-graph".equals(args[i]))
                graphName = args[++i];
            else if("-engine".equals(args[i]))
                engineURL = args[++i];
            else if(i == 1 && "ontology".equals(args[i]))
                ontology = true;
            else if(i == 1 && "data".equals(args[i]))
                data = true;
            else if(i == 0 && "export".equals(args[i]))
               continue;
            else
                die("Unknown option " + args[i]);
        }

        engineURL = engineURL == null ? Mindmaps.DEFAULT_URI : engineURL;
        if(graphName == null){
            die("You must provide a graph name argument using -graph");
        }

        System.out.println("Writing graph " + graphName + " using MM Engine " +
                engineURL + " to file " + (outputFileName == null ? "System.out" : outputFileName));

        MindmapsGraph graph = Mindmaps.factory(engineURL, graphName).getGraph();
        GraphWriter writer = new GraphWriter(graph);

        String contents = null;
        if(ontology){
            contents = writer.dumpOntology();
        } else if(data){
            contents = writer.dumpData();
        }

        if(outputFileName == null){
            System.out.println(contents);
            return;
        }

        try{
            File outputFile = new File(outputFileName);
            outputFile.createNewFile();

            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile));
            bufferedWriter.write(contents);
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e){
            die("Problem writing to file " + outputFileName);
        }
    }
}
