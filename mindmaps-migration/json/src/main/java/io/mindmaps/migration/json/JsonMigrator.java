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

import io.mindmaps.engine.loader.Loader;
import io.mindmaps.graql.Graql;
import io.mindmaps.migration.base.AbstractTemplatedMigrator;
import mjson.Json;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Migrator for migrating JSON data into Mindmaps instances
 */
public class JsonMigrator extends AbstractTemplatedMigrator {

    private static final String NEWLINE = "\n";

    /**
     * Create a JsonMigrator to migrate into the given graph
     */
    public JsonMigrator(Loader loader) {
        this.loader = loader;
    }

    @Override
    public void migrate(String template, File jsonFileOrDir){
        checkBatchSize();

        File[] files = {jsonFileOrDir};
        if(jsonFileOrDir.isDirectory()){
            files = jsonFileOrDir.listFiles(jsonFiles);
        }

        try {
            resolve(template, Arrays.stream(files)).forEach(loader::addToQueue);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            loader.flush();
            loader.waitToFinish();
        }
    }

    @Override
    public String graql(String template, File file){
        File[] files = {file};
        if(file.isDirectory()){
            files = file.listFiles(jsonFiles);
        }

        try {
            return resolve(template, Arrays.stream(files)).collect(joining(NEWLINE));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert native data format to a stream of templates
     * @param template parametrized graql insert query
     * @param files
     * @return
     */
    private Stream<String> resolve(String template, Stream<File> files){
        Stream<Json> json = files.map(this::convertFile);

        return partitionedStream(json.iterator())
                .map(this::batchParse)
                .map(data -> template(template, data));
    }

    /**
     * Call parse of a collection of input data
     */
    private Collection<Map<String, Object>> batchParse(Collection<Json> batch){
        return batch.stream().map(this::parse).collect(toList());
    }

    /**
     * Convert data in JSON object to a Map<String, Object>, the current templating input.
     * There is a direct mapping between any JSON object and a Map.
     * @param json json to convert
     * @return converted json map
     */
    private Map<String, Object> parse(Json json){
        return json.asMap();
    }

    private String template(String template, Collection<Map<String, Object>> data){
        Map<String, Object> forData = Collections.singletonMap("data", data);
        template = "for (data) do { " + template + "}";
        return  "insert " + Graql.parseTemplate(template, forData);
    }

    /**
     * Convert a file into a Json object
     * @param file file to be converted
     * @return Json object representing the file, empty if problem reading file
     */
    private Json convertFile(File file){
        try {
            return Json.read(file.toURI().toURL());
        } catch (MalformedURLException e){
            LOG.warn("Problem reading Json file " + file.getPath());
            return Json.object();
        }
    }

    /**
     * Filter that will only accept JSON files with the .json extension
     */
    private FilenameFilter jsonFiles = (dir, name) -> name.toLowerCase().endsWith(".json");
}
