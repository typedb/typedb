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

import ai.grakn.migration.base.io.MigrationCLI;

import java.io.File;
import java.util.Optional;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static ai.grakn.migration.base.io.MigrationCLI.fileAsString;
import static ai.grakn.migration.base.io.MigrationCLI.initiateShutdown;
import static ai.grakn.migration.base.io.MigrationCLI.printInitMessage;
import static ai.grakn.migration.base.io.MigrationCLI.printWholeCompletionMessage;
import static ai.grakn.migration.base.io.MigrationCLI.writeToSout;

/**
 * Main program to migrate XML data into a Grakn graph. For use from a command line.
 * Expected arguments are the XML file and the Grakn graph name.
 * 
 * @author alexandraorth, boris
 */
public class Main {

    public static void main(String[] args) {
        MigrationCLI.init(args, XmlMigrationOptions::new).stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(Main::runXml);
    }

    public static void runXml(XmlMigrationOptions options){
        File xmlDataFile = new File(options.getInput());
        File xmlTemplateFile = new File(options.getTemplate());

        if(!xmlDataFile.exists()){
            die("Cannot find file: " + options.getInput());
        }

        if(!xmlTemplateFile.exists() || xmlTemplateFile.isDirectory()){
            die("Cannot find file: " + options.getTemplate());
        }

        printInitMessage(options, xmlDataFile.getPath());

        String template = fileAsString(xmlTemplateFile);
        try(XmlMigrator xmlMigrator = new XmlMigrator(template, xmlDataFile)){
            if (options.getElement() != null)
                xmlMigrator.element(options.getElement());
            else
                die("Please specify XML element for the top-level data item.");
            if (options.getSchemaFile() != null)
                xmlMigrator.schema(new XmlSchema().read(new File(options.getSchemaFile())));
            if(options.isNo()){
                writeToSout(xmlMigrator.migrate());
            } else {
                xmlMigrator.load(options.getUri(), options.getKeyspace(), options.getBatch(), options.getNumberActiveTasks(), options.getRetry());
                printWholeCompletionMessage(options);
            }
        } catch (Throwable throwable){
            die(throwable);
        }

        initiateShutdown();
    }
}
