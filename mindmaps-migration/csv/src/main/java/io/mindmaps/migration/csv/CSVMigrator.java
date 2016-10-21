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

import com.opencsv.CSVReader;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.graql.Graql;
import io.mindmaps.migration.base.AbstractTemplatedMigrator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * The CSV data migrator will migrate all of the data in a CSV file into Mindmaps Graql var patters, to be
 * imported into a graph as the user sees fit.
 */
public class CSVMigrator extends AbstractTemplatedMigrator {

    public static final char DELIMITER = ',';
    private static final String NEWLINE = "\n";
    private char delimiter = DELIMITER;

    /**
     * Construct a CSV migrator
     * @param loader loader for the migrator to use
     */
    public CSVMigrator(Loader loader){
        this.loader = loader;
        loader.setBatchSize(1);
    }

    /**
     * Set delimiter the input file will be split on
     * @param delimiter character separating columns in input
     */
    public CSVMigrator setDelimiter(char delimiter){
        this.delimiter = delimiter;
        return this;
    }

    @Override
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

    @Override
    public String graql(String template, File file){
        try (CSVReader reader =  new CSVReader(new FileReader(file), delimiter, '"', 0)){
            return resolve(template, reader).collect(joining(NEWLINE));
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert native data format (CSVReader) to stream of templates
     * @param template parametrized graql insert query
     * @param reader CSV reader of data file
     * @return Stream of data migrated into templates
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
        template = "for(data) do { " + template + "}";
        return "insert " + Graql.parseTemplate(template, forData);
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
