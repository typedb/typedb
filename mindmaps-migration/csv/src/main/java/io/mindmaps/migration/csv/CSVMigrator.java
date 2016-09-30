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
import scala.Char;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * The CSV data migrator will migrate all of the data in a CSV file into Mindmaps Graql var patters, to be
 * imported into a graph as the user sees fit.
 *
 * This class implements Iterator.
 */
public class CSVMigrator {

    private static final char COMMA = ',';
    private static final char NEWLINE = '\n';
    private final Loader loader;
    private final char delimiter;

    public CSVMigrator(Loader loader){
        this(loader, COMMA);
    }

    public CSVMigrator(Loader loader, char delimiter){
        this.delimiter = delimiter;
        this.loader = loader;

        loader.setBatchSize(1);

        System.out.println(loader.getBatchSize());
    }

    public void migrate(String template, File file){

        try (CSVReader reader =  new CSVReader(new FileReader(file), delimiter, '"', 0)){
           resolve(template, reader).forEach(loader::addToQueue);
        } catch (IOException e){
            throw new RuntimeException(e);
        } finally {
            loader.flush();
            loader.waitToFinish();
        }
    }

    public String graql(String template, File file){

        try (CSVReader reader =  new CSVReader(new FileReader(file), delimiter, '"', 0)){
            return resolve(template, reader).collect(joining(Character.toString(NEWLINE)));
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private Stream<String> resolve(String template, CSVReader reader) throws IOException {

        String[] header = reader.readNext();

        return StreamSupport.stream(reader.spliterator(), false)
                .map(line -> parse(header, line))
                .map(data -> resolve(template, data));
    }

    private String resolve(String template, Map<String, Object> data){
        System.out.println("insert " + Graql.parseTemplate(template, data));
        return "insert " + Graql.parseTemplate(template, data);
    }

    private Map<String, Object> parse(String[] header, String[] data){
        if(header.length != data.length){
            throw new RuntimeException("Invalid CSV");
        }

        return IntStream.range(0, header.length)
                .mapToObj(Integer::valueOf)
                .collect(toMap(
                        i -> header[i],
                        i -> data[i]
                ));
    }
}
