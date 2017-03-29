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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.migration.base.MigrationCLI;
import org.semanticweb.owlapi.apibinding.OWLManager;

import java.io.File;
import java.util.Optional;

import static ai.grakn.migration.base.MigrationCLI.die;
import static ai.grakn.migration.base.MigrationCLI.printInitMessage;
import static ai.grakn.migration.base.MigrationCLI.printWholeCompletionMessage;

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
 * @author alexandraorth
 *
 */
public class Main {

    public static void main(String[] args) {
        MigrationCLI.init(args, OwlMigrationOptions::new).stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(Main::runOwl);
    }

    public static void runOwl(OwlMigrationOptions options){
        File owlfile = new File(options.getInput());
        if (!owlfile.exists()) {
            die("Cannot find file: " + options.getInput());
        }

        printInitMessage(options, owlfile.getPath());

        OWLMigrator migrator = new OWLMigrator();
        try(GraknGraph graph = Grakn.session(options.getUri(), options.getKeyspace()).open(GraknTxType.WRITE)) {
            migrator.graph(graph)
                    .ontology(OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(owlfile))
                    .migrate();

            printWholeCompletionMessage(options);
        }
        catch (Throwable t) {
            die(t);
        }
    }
}
