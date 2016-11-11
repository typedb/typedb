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
package ai.grakn.migration.owl;

import ai.grakn.migration.base.io.MigrationCLI;
import ai.grakn.migration.base.io.MigrationCLI;
import org.apache.commons.cli.Options;
import org.semanticweb.owlapi.apibinding.OWLManager;

import java.io.File;

/**
 * <p>
 * Main program to migrate an OWL knowledge base into a Grakn knowledge graph. Expected
 * arguments are an OWL file and a Grakn Engine URL. At a minimum an OWL file must be provided.
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

    private static Options options = new Options();
    static {
        options.addOption("i", "input", true, "input owl file");
    }

    public static void main(String[] args) {
        MigrationCLI.create(args, options).ifPresent(Main::runOwl);
    }

    public static void runOwl(MigrationCLI cli){

        String owlFilename = cli.getRequiredOption("input", "Please specify owl file with the -i option.");

        File owlfile = new File(owlFilename);
        if (!owlfile.exists())
            cli.die("Cannot find file: " + owlFilename);

        cli.printInitMessage(owlfile.getPath());

        OWLMigrator migrator = new OWLMigrator();
        try {
            migrator.graph(cli.getGraph())
                    .ontology(OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(owlfile))
                    .migrate();

            cli.printWholeCompletionMessage();
        }
        catch (Throwable t) {
            cli.die(t);
        }
        finally {
            if (migrator.graph() != null)
                migrator.graph().close();
        }

        cli.initiateShutdown();
    }
}
