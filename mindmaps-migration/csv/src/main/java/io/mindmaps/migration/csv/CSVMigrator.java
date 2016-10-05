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

package io.mindmaps.migration.csv;

import com.google.common.collect.Iterators;
import com.opencsv.CSVReader;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.graql.Graql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * The CSV data migrator will migrate all of the data in a CSV file into Mindmaps Graql var patters, to be
 * imported into a graph as the user sees fit.
 */
public class CSVMigrator {

    private static Logger LOG = LoggerFactory.getLogger(CSVMigrator.class);

    public static final int BATCH_SIZE = 5;
    public static final char DELIMITER = ',';
    private static final String NEWLINE = "\n";

    private final Loader loader;
    private char delimiter = DELIMITER;
    private int batchSize = BATCH_SIZE;

    /**
     * Construct a CSV migrator
     * @param loader loader for the migrator to use
     */
    public CSVMigrator(Loader loader){
        this.loader = loader;
        loader.setBatchSize(1);
    }

    /**
     * Set number of rows to migrate in one batch
     * @param batchSize number of rows to migrate at once
     */
    public CSVMigrator setBatchSize(int batchSize){
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Set delimiter the input file will be split on
     * @param delimiter character separating columns in input
     */
    public CSVMigrator setDelimiter(char delimiter){
        this.delimiter = delimiter;
        return this;
    }

    /**
     * Migrate all the data in the given file based on the given template.
     * @param template parametrized graql insert query
     * @param file file containing data to be migrated
     */
    public void migrate(String template, File file){
        checkBatchSize();

        try (CSVReader reader =  new CSVReader(new FileReader(file), delimiter, '"', 0)){
           resolve(template, reader).forEach(loader::addToQueue);
        } catch (IOException e){
            throw new RuntimeException(e);
        } finally {
            loader.flush();
            loader.waitToFinish();
        }
    }

    /**
     * Migrate all the data in the given file based on the given template.
     * @param template parametrized graql insert query
     * @param file file containing data to be migrated
     * @return Graql insert statement representing the file
     */
    public String graql(String template, File file){
        checkBatchSize();

        try (CSVReader reader =  new CSVReader(new FileReader(file), delimiter, '"', 0)){
            return resolve(template, reader).collect(joining(NEWLINE));
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Conevrt native data format (CSVReader) to stream of templates
     * @param template
     * @param reader
     * @return
     * @throws IOException
     */
    private Stream<String> resolve(String template, CSVReader reader) throws IOException {
        String[] header = reader.readNext();

        return partitionedStream(reader.iterator())
                .map(batch -> batchParse(header, batch))
                .map(data -> template(template, data));
    }

    /**
     * Call parse of a collection of input data
     */
    private Collection<Map<String, Object>> batchParse(String[] header, Collection<String[]> batch){
        return batch.stream().map(line -> parse(header, line)).collect(toList());
    }

    /**
     * Convert data in arrays (from CSV reader) to Map<String, Object>, the current input format for
     * graql templating.
     * @param header first row of input file, representing keys in the template
     * @param data all bu first row of input file
     * @return given data in a map
     */
    private Map<String, Object> parse(String[] header, String[] data){
        if(header.length != data.length){
            throw new RuntimeException("Invalid CSV");
        }

        return IntStream.range(0, header.length)
                .mapToObj(Integer::valueOf)
                .filter(i -> validValue(data[i]))
                .collect(toMap(
                        i -> header[i],
                        i -> data[i]
                ));
    }

    private String template(String template, Collection<Map<String, Object>> data){
        Map<String, Object> forData = Collections.singletonMap("data", data);
        template = "for{data} do { " + template + "}";
        return "insert " + Graql.parseTemplate(template, forData);
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
     * Partition a stream into a stream of collections, each with batchSize elements.
     * @param iterator Iterator to partition
     * @param <T> Type of values of iterator
     * @return Stream over a collection that are each of batchSize
     */
    private <T> Stream<Collection<T>> partitionedStream(Iterator<T> iterator){
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize(
                Iterators.partition(iterator, batchSize), Spliterator.ORDERED), false);
    }

    /**
     * Test if an object is a valid Mindmaps value
     * @param value object to check
     * @return if the value is valid
     */
    private boolean validValue(Object value){
        return value != null && !value.toString().isEmpty();
    }
}
