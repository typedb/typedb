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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.base.io.MigrationLoader;
import ai.grakn.migration.csv.CSVMigrator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.stream.Collectors.joining;

public class GenealogyGraph extends TestGraph{

    final static String ontologyFile = "genealogy/ontology.gql";

    final static String peopleTemplatePath = filePath + "genealogy/person-migrator.gql";
    final static String peoplePath = filePath + "genealogy/people.csv";

    final static String parentTemplatePath = filePath + "genealogy/parentage-migrator.gql";
    final static String parentageFilePath = filePath + "genealogy/parentage.csv";

    final static String marriageTemplatePath = filePath + "genealogy/marriage-migrator.gql";
    final static String marriageFilePath = filePath + "genealogy/marriages.csv";

    final static String ruleFile = "genealogy/events-to-genealogy-rules.gql";
    final static String ruleFile2 = "genealogy/role-genderization-rules.gql";
    final static String ruleFile3 = "genealogy/inferred-kinships.gql";

    public GenealogyGraph(){
        super(null, ontologyFile, ruleFile, ruleFile2, ruleFile3);
        try {
            String peopleTemplate = getResourceAsString(peopleTemplatePath);
            String parentTemplate = getResourceAsString(parentTemplatePath);
            String marriageTemplate = getResourceAsString(marriageTemplatePath);
            File peopleFile = new File(peoplePath);
            File parentFile = new File(parentageFilePath);
            File marriageFile = new File(marriageFilePath);

            // create a migrator with your macro
            Migrator personMigrator = new CSVMigrator(peopleTemplate, peopleFile);
            MigrationLoader.load(graph(), personMigrator);

            Migrator parentMigrator = new CSVMigrator(parentTemplate, parentFile);
            MigrationLoader.load(graph(), parentMigrator);

            Migrator marriageMigrator = new CSVMigrator(marriageTemplate, marriageFile);
            MigrationLoader.load(graph(), marriageMigrator);
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public static Path getResource(String resourceName){
        return Paths.get(resourceName);
    }

    public static String getResourceAsString(String resourceName) throws IOException {
        return Files.readAllLines(getResource(resourceName)).stream().collect(joining("\n"));
    }
}
