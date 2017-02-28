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

package ai.grakn.migration.json;

import ai.grakn.migration.base.MigrationCLI;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import mjson.Json;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.migration.base.MigrationCLI.die;
import static ai.grakn.migration.base.MigrationCLI.printInitMessage;
import static java.util.stream.Collectors.toSet;

/**
 * Migrator for migrating JSON data into Grakn instances
 * @author alexandraorth
 */
public class JsonMigrator implements AutoCloseable {

    private final Set<Reader> readers;

    public static void main(String[] args) {
        MigrationCLI.init(args, JsonMigrationOptions::new).stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(JsonMigrator::runJson);
    }

    public static void runJson(JsonMigrationOptions options){
        File jsonDataFile = new File(options.getInput());
        File jsonTemplateFile = new File(options.getTemplate());

        if(!jsonDataFile.exists()){
            die("Cannot find file: " + options.getInput());
        }

        if(!jsonTemplateFile.exists() || jsonTemplateFile.isDirectory()){
            die("Cannot find file: " + options.getTemplate());
        }

        printInitMessage(options, jsonDataFile.getPath());

        try(JsonMigrator jsonMigrator = new JsonMigrator(jsonDataFile)){
            MigrationCLI.loadOrPrint(jsonTemplateFile, jsonMigrator.convert(), options);
        } catch (Throwable throwable){
            die(throwable);
        }
    }

    /**
     * Construct a JsonMigrator to migrate data in the given file or dir
     * @param jsonFileOrDir either a Json file or a directory containing Json files
     */
    public JsonMigrator(File jsonFileOrDir){
        File[] files = {jsonFileOrDir};
        if(jsonFileOrDir.isDirectory()){

            // Filter that will only accept JSON files with the .json extension
            FilenameFilter jsonFiles = (dir, name) -> name.toLowerCase().endsWith(".json");
            files = jsonFileOrDir.listFiles(jsonFiles);
        }

        this.readers = Stream.of(files).map(this::asReader).collect(toSet());
    }

    /**
     * Construct a JsonMigrator to migrate data in given reader
     * @param reader reader over the data to be migrated
     */
    public JsonMigrator(Reader reader){
        this.readers = Sets.newHashSet(reader);
    }

    /**
     * Migrate each of the given json objects as an insert query
     * @return stream of parsed insert queries
     */
    public Stream<Map<String, Object>> convert(){
        return readers.stream()
                .map(this::asString)
                .map(this::toJsonMap);
    }

    /**
     * Close the readers
     */
    @Override
    public void close() {
        readers.forEach((reader) -> {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Convert data in JSON object to a Map<String, Object>, the current templating input.
     * There is a direct mapping between any JSON object and a Map.
     * @param data data to convert
     * @return converted json map
     */
    private Map<String, Object> toJsonMap(String data){
        return Json.read(data).asMap();
    }

    /**
     * Convert a reader to a string
     * @param reader reader to be converted
     * @return Json object representing the file, empty if problem reading file
     */
    private String asString(Reader reader){
        try {
            return CharStreams.toString(reader);
        } catch (IOException e){
            throw new RuntimeException("Problem reading input");
        }
    }

    /**
     * Convert a file into a Reader
     * @param file file to be converted
     * @return Json object representing the file, empty if problem reading file
     */
    private InputStreamReader asReader(File file){
        try {
            return new InputStreamReader(new FileInputStream(file), Charset.defaultCharset());
        } catch (IOException e){
            throw new RuntimeException("Problem reading input");
        }
    }
}
