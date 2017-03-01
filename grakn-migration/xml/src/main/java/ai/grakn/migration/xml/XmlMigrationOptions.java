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

package ai.grakn.migration.xml;

import ai.grakn.migration.base.MigrationOptions;
import ai.grakn.migration.base.Migrator;

import static java.lang.Integer.parseInt;

/**
 * Configure the default XML migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class XmlMigrationOptions extends MigrationOptions {

    private final String batch = Integer.toString(Migrator.BATCH_SIZE);
    private final String active = Integer.toString(Migrator.ACTIVE_TASKS);

    public XmlMigrationOptions(String[] args) {
        super();

        options.addOption("i", "input", true, "Input XML data file or directory.");
        options.addOption("s", "schema", true, "The XML Schema file name, usually .xsd extension defining with type information about the data.");        
        options.addOption("e", "element", true, "The name of the XML element to migrate - all others will be ignored.");
        options.addOption("t", "template", true, "Graql template to apply to the data.");
        options.addOption("b", "batch", true, "Number of rows to execute in one Grakn transaction. Default 25.");
        options.addOption("a", "active", true, "Number of tasks (batches) running on the server at any one time. Default 25.");

        parse(args);
    }

    public String getElement() {
        return command.getOptionValue("e", null);
    }

    public String getSchemaFile() {
        return command.getOptionValue("s", null);
    }
    
    public int getBatch() {
        return parseInt(command.getOptionValue("b", batch));
    }

    public int getNumberActiveTasks() {
        return parseInt(command.getOptionValue("a", active));
    }
}
