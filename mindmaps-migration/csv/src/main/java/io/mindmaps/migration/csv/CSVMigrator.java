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
import io.mindmaps.graql.Var;
import io.mindmaps.migration.base.AbstractMigrator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * The CSV data migrator will migrate all of the data in a CSV file into Mindmaps Graql var patters, to be
 * imported into a graph as the user sees fit.
 */
public class CSVMigrator extends AbstractMigrator {

    public static final char DELIMITER = ',';
    private char delimiter = DELIMITER;

    /**
     * Set delimiter the input file will be split on
     * @param delimiter character separating columns in input
     */
    public CSVMigrator setDelimiter(char delimiter){
        this.delimiter = delimiter;
        return this;
    }

    @Override
    public Stream<Collection<Var>> migrate(String template, File file){
        try (FileReader reader = new FileReader(file)){
            return migrate(template, reader);
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Each String in the stream is a CSV file
     * @param template parametrized graql insert query
     * @param reader reader over the data to be migrated
     * @return
     */
    @Override
    public Stream<Collection<Var>> migrate(String template, final Reader reader) {
        try(CSVReader csvReader = new CSVReader(reader, delimiter, '"', 0)) {

            Iterator<String[]> it = csvReader.iterator();

            String[] header = it.next();

            return partitionedStream(it).collect(toList()).stream()
                    .map(col -> batchParse(header, col))
                    .map(col -> template(template, col));
        } catch (IOException e){
            throw new RuntimeException(e);
        }
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

    /**
     * Test if an object is a valid Mindmaps value
     * @param value object to check
     * @return if the value is valid
     */
    private boolean validValue(Object value){
        return value != null && !value.toString().isEmpty();
    }
}
