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
package io.mindmaps.migration.owl;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RelationType;
import io.mindmaps.factory.MindmapsClient;
import org.semanticweb.owlapi.apibinding.OWLManager;

import java.io.File;

/**
 * <p>
 * Main program to migrate an OWL knowledge base into a Mindmaps knowledge graph. Expected 
 * arguments are an OWL file and a Mindmaps Engine URL. At a minimum an OWL file must be provided. 
 * Note that the OWLAPI is not very good at intelligently resolving imports, such as looking in the
 * same folder etc. To import a large ontology made up of multiple imports scattered around in files, 
 * the easiest thing is to use protege to "merge" them into a single ontology file with all axioms 
 * inside it.
 * </p>
 * 
 * @author borislav
 *
 */
public class Main {
    
    static void die(String errorMsg) {
        System.out.println(errorMsg);
        System.out.println("\nSyntax: OWLMigrator -owl <owl filename> [-graph <graph name>] [-engine <Mindmaps engine URL>]");
        System.exit(-1);
    }
        
    public static void main(String[] argv) {
        String owlFilename = null;
        String engineUrl = null;
        String graphName = null;
        
        for (int i = 0; i < argv.length; i++) {
            if ("-owl".equals(argv[i]))
                owlFilename = argv[++i];
            else if ("-graph".equals(argv[i]))
                graphName = argv[++i];
            else if ("-engine".equals(argv[i]))
                engineUrl = argv[++i];
            else
                die("Unknown option " + argv[i]);
        }       
        
        if (owlFilename == null)
            die("Please specify owl file with the -owl option.");
        File owlfile = new File(owlFilename);
        if (!owlfile.exists())
            die("Cannot find file: " + owlFilename);
        if (graphName == null)
            graphName = owlfile.getName().replace(".", "_");
        
        System.out.println("Migrating " + owlFilename + " using MM Engine " + 
                            (engineUrl == null ? "local" : engineUrl ) + " into graph " + graphName);
        
        OWLMigrator migrator = new OWLMigrator();
        
        try {
            MindmapsGraph graph = engineUrl == null ? MindmapsClient.getGraph(graphName)
                                                    : MindmapsClient.getGraph(graphName, engineUrl);            
            migrator.graph(graph)
                    .ontology(OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(owlfile))
                    .migrate();
            System.out.println("Migration successfully completed!");
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(-1);
        }
        finally {
            if (migrator.graph() != null)
                migrator.graph().close();
        }
    }

    // stub to test and throw away stuff
    static void test() {
        try {
            MindmapsGraph graph = MindmapsClient.getGraph("onco");
            graph.putRelationType("authorship");
            RelationType reltype = (RelationType)graph.getType("relation-type").instances().stream().findFirst().get();
            reltype.hasRoles().forEach(System.out::println);
            reltype.hasRole(graph.putRoleType("authro1")).hasRole(graph.putRoleType("author2"));
            graph.commit();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
        }
        System.exit(0);
    }
}
