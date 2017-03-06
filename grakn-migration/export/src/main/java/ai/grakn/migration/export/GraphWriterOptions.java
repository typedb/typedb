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

import ai.grakn.migration.base.MigrationOptions;

/**
 * Configure the default export options and access arguments passed by the user
 * @author alexandraorth
 */
public class GraphWriterOptions extends MigrationOptions {

    public GraphWriterOptions(String[] args) {
        super();

        options.addOption("ontology", false, "export ontology");
        options.addOption("data", false, "export data");

        parse(args);
    }

    public boolean exportOntology(){
        return command.hasOption("ontology");
    }

    public boolean exportData(){
        return command.hasOption("data");
    }
}
