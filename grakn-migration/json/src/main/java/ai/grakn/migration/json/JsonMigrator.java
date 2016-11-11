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

import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import ai.grakn.graql.InsertQuery;
import ai.grakn.migration.base.AbstractMigrator;
import mjson.Json;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * Migrator for migrating JSON data into Grakn instances
 */
public class JsonMigrator extends AbstractMigrator {

    private final Set<Reader> readers;
    private final String template;

    /**
     * Construct a JsonMigrator to migrate data in the given file or dir
     * @param template parametrized graql insert query
     * @param jsonFileOrDir either a Json file or a directory containing Json files
     */
    public JsonMigrator(String template, File jsonFileOrDir){
        File[] files = {jsonFileOrDir};
        if(jsonFileOrDir.isDirectory()){
            files = jsonFileOrDir.listFiles(jsonFiles);
        }

        this.readers = Stream.of(files).map(this::asReader).collect(toSet());
        this.template = template;
    }

    /**
     * Construct a JsonMigrator to migrate data in given reader
     * @param template parametrized graql insert query
     * @param reader reader over the data to be migrated
     */
    public JsonMigrator(String template, Reader reader){
        this.readers = Sets.newHashSet(reader);
        this.template = template;
    }

    /**
     * Migrate each of the given json objects as an insert query
     * @return stream of parsed insert queries
     */
    @Override
    public Stream<InsertQuery> migrate(){
        return readers.stream()
                .map(this::asString)
                .map(this::toJsonMap)
                .map(data -> template(template, data));
    }

    /**
     * Close the readers
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
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
    private FileReader asReader(File file){
        try {
            return new FileReader(file);
        } catch (IOException e){
            throw new RuntimeException("Problem reading input");
        }
    }

    /**
     * Filter that will only accept JSON files with the .json extension
     */
    private FilenameFilter jsonFiles = (dir, name) -> name.toLowerCase().endsWith(".json");
}
