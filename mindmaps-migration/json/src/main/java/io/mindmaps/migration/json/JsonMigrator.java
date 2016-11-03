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

package io.mindmaps.migration.json;

import com.google.common.io.CharStreams;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.migration.base.AbstractMigrator;
import mjson.Json;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Migrator for migrating JSON data into Mindmaps instances
 */
public class JsonMigrator extends AbstractMigrator {

    @Override
    public  Stream<InsertQuery> migrate(String template, File jsonFileOrDir) {
        File[] files = {jsonFileOrDir};
        if(jsonFileOrDir.isDirectory()){
            files = jsonFileOrDir.listFiles(jsonFiles);
        }

        Stream<String> jsonObjects = Stream.of(files).
                map(this::asReader)
                .map(this::asString);

        return stream(jsonObjects.map(this::toJsonMap).iterator())
                .map(i -> template(template, i));
    }

    /**
     * Readers contains a Json object
     * @param template parametrized graql insert query
     * @param reader reader over the data to be migrated
     */
    @Override
    public Stream<InsertQuery> migrate(String template, Reader reader){
        return Stream.of(template(template, toJsonMap(asString(reader))));
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
