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

package ai.grakn.migration.json;

import ai.grakn.engine.loader.Loader;
import com.google.common.collect.Iterators;
import ai.grakn.engine.loader.Loader;
import ai.grakn.graql.Graql;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * Migrator for migrating JSON data into Mindmaps instances
 */
public class JsonMigrator {

    public static final int BATCH_SIZE = 5;
    private static Logger LOG = LoggerFactory.getLogger(JsonMigrator.class);

    private final Loader loader;
    private int batchSize = BATCH_SIZE;


    /**
     * Create a JsonMigrator to migrate into the given graph
     */
    public JsonMigrator(Loader loader) {
        this.loader = loader;
    }

    /**
     * Set number of files/objects to migrate in one batch
     * @param batchSize number of objects to migrate at once
     */
    public JsonMigrator setBatchSize(int batchSize){
        this.batchSize = batchSize;
        return this;
    }

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

    public void graql(String template, File file){
        checkBatchSize();


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
     * Warn the user when the batch size of the loader is greater than 1.
     * If the batch size is greater than 1, it is possible that multiple of the same variables will be committed in
     * one batch and the resulting committed data will be corrupted.
     */
    private void checkBatchSize(){
        if(loader.getBatchSize() > 1){
            LOG.warn("Loading with batch size [" + loader.getBatchSize() + "]. This can cause conflicts on commit.");
        }
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

    /**
     * Partition a stream into a stream of collections, each with batchSize elements.
     * @param iterator Iterator to partition
     * @param <T> Type of values of iterator
     * @return Stream over a collection that are each of batchSize
     */
    private <T> Stream<Collection<T>> partitionedStream(Iterator<T> iterator){
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                Iterators.partition(iterator, batchSize), Spliterator.ORDERED), false);
    }
}
