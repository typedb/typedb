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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * The CSV migrator will migrate all of the data in a CSV file into Grakn Graql var patters, to be
 * imported into a graph as the user sees fit.
 * @author alexandraorth
 */
public class CSVMigrator extends AbstractMigrator {

    public static final char SEPARATOR = ',';
    public static final char QUOTE = '\"';
    public static final String NULL_STRING = null;
    private char separator = SEPARATOR;
    private char quote = QUOTE;
    private String nullString = NULL_STRING;

    private final Reader reader;
    private final String template;

    /**
     * Construct a CSVMigrator to migrate data in the given file
     * @param template parametrized graql insert query
     * @param file file with the data to be migrated
     */
    public CSVMigrator(String template, File file) {
        try {
            this.reader = new InputStreamReader(new FileInputStream(file), Charset.defaultCharset());
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
     * Set character used to encapsulate values containing special characters.
     * @param quote the quote character
     */
    public CSVMigrator setQuoteChar(char quote){
        this.quote = quote;
        return this;
    }

    /**
     * Set string that will be evaluated as null
     * @param nullString string that will be evaluated as null, if null, everything will be
     *                   evaluated as a string
     */
    public CSVMigrator setNullString(String nullString){
        this.nullString = nullString;
        return this;
    }

    /**
     * Each String in the stream is a CSV file
     * @return stream of parsed insert queries
     */
    @Override
    public Stream<InsertQuery> migrate() {
        try{
                CSVParser csvParser = CSVFormat.newFormat(separator)
                            .withIgnoreEmptyLines()
                            .withEscape('\\' )
                            .withFirstRecordAsHeader()
                            .withQuote(quote)
                            .withNullString(nullString)
                            .parse(reader);

            Iterator<CSVRecord> it = csvParser.iterator();
            return stream(it)
                    .map(col -> template(template, parse(col)))
                    .filter(Optional::isPresent)
                    .map(Optional::get);
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
     * @param data all bu first row of input file
     * @return given data in a map
     */
    private Map<String, Object> parse(CSVRecord data){
        if(!data.isConsistent()){
            throw new RuntimeException("Invalid CSV " + data.toMap());
        }
        return data.toMap().entrySet().stream()
                .filter((e) -> validValue(e.getValue()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
