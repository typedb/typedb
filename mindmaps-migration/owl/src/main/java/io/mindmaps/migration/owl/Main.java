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

import io.mindmaps.migration.base.io.MigrationCLI;
import org.semanticweb.owlapi.apibinding.OWLManager;

import java.io.File;

import static io.mindmaps.migration.base.io.MigrationCLI.die;

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

    static {
        MigrationCLI.addOption("f", "file", true, "owl file");
    }

    public static void main(String[] args) {

        MigrationCLI interpreter = new MigrationCLI(args);

        String owlFilename = interpreter.getRequiredOption("f", "Please specify owl file with the -owl option.");

        File owlfile = new File(owlFilename);
        if (!owlfile.exists())
            die("Cannot find file: " + owlFilename);

        interpreter.printInitMessage(owlfile.getPath());

        OWLMigrator migrator = new OWLMigrator();
        try {
            migrator.graph(interpreter.getGraph())
                    .ontology(OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(owlfile))
                    .migrate();

            interpreter.printCompletionMessage();
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
}
