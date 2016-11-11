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

package ai.grakn.migration.csv;

import ai.grakn.graql.InsertQuery;
import ai.grakn.migration.base.AbstractMigrator;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import ai.grakn.graql.InsertQuery;
import ai.grakn.migration.base.AbstractMigrator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * The CSV migrator will migrate all of the data in a CSV file into Grakn Graql var patters, to be
 * imported into a graph as the user sees fit.
 */
public class CSVMigrator extends AbstractMigrator {

    public static final char SEPARATOR = ',';
    private char separator = SEPARATOR;

    private final Reader reader;
    private final String template;

    /**
     * Construct a CSVMigrator to migrate data in the given file
     * @param template parametrized graql insert query
     * @param file file with the data to be migrated
     */
    public CSVMigrator(String template, File file) {
        try {
            this.reader = new FileReader(file);
            this.template = template;
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct a CSVMigrator to migrate data in the given Reader
     * @param template parametrized graql insert query
     * @param reader reader over the data to be migrated
     */
    public CSVMigrator(String template, Reader reader){
        this.reader = reader;
        this.template = template;
    }

    /**
     * Set separator the input file will be split on
     * @param separator character separating columns in input
     */
    public CSVMigrator setSeparator(char separator){
        this.separator = separator;
        return this;
    }

    /**
     * Each String in the stream is a CSV file
     * @return stream of parsed insert queries
     */
    @Override
    public Stream<InsertQuery> migrate() {
        try(
                CSVReader csvReader =
                        new CSVReader(reader, 0, new CSVParserBuilder()
                                .withSeparator(separator)
                                .withIgnoreLeadingWhiteSpace(true)
                                .withEscapeChar('\\')
                                .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                                .build())

        ) {

            Iterator<String[]> it = csvReader.iterator();

            String[] header = it.next();

            //TODO don't collect here
            return stream(it).collect(toList()).stream()
                    .map(col -> parse(header, col))
                    .map(col -> template(template, col));
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Close the reader
     */
    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
}
