/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package configure;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 *  Contains the configuration for an execution of the benchmarking system
 */

public class BenchmarkConfiguration {

    private QueriesConfigurationFile queries;
    private List<String> schemaGraql;
    private boolean noSchemaLoad = true;
    private boolean noDataGeneration = true;
    private BenchmarkConfigurationFile benchmarkConfigFile;
    private Path configFilePath;

    public BenchmarkConfiguration(Path configFilePath, BenchmarkConfigurationFile config) throws IOException {
        this.configFilePath = configFilePath;
        this.benchmarkConfigFile = config;

        // read the queries file string and use them to load further YAML
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Path queryFilePath = this.configFilePath.getParent().resolve(config.getRelativeQueriesYamlFile());
        queries = mapper.readValue(queryFilePath.toFile(), QueriesConfigurationFile.class);

        try {
            Path schemaFilePath = this.configFilePath.getParent().resolve(config.getRelativeSchemaFile());
            schemaGraql = Files.readLines(schemaFilePath.toFile(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName() {
        return this.benchmarkConfigFile.getName();
    }

    public String getDefaultKeyspace() {
        String name = this.getName();
        // remove spaces
        name = name.replace(' ', '_');
        if (name.length() > 48) {
            return name.substring(0, 48);
        } else {
            return name;
        }
    }

    public List<String> getSchema() {
        if (this.noSchemaLoad) {
            return null;
        } else {
            return this.schemaGraql;
        }
    }

    public List<String> getQueries() {
        return this.queries.getQueries();
    }

    public List<Integer> getConceptsToBenchmark() {
        if (this.noDataGeneration) {
            return null;
        } else {
            return this.benchmarkConfigFile.getConceptsToBenchmark();
        }
    }

    public void setNoSchemaLoad(boolean loadSchema) {
        this.noSchemaLoad = loadSchema;
    }
    public boolean noSchemaLoad() {
        // we also don't load the schema
        // if the data generation is disabled
        return this.noDataGeneration || this.noSchemaLoad;
    }

    public void setNoDataGeneration(boolean generateData) {
        this.noDataGeneration = generateData;
    }
    public boolean noDataGeneration() {
        return this.noDataGeneration;
    }

    public int noQueryRepetitions() {
        return this.benchmarkConfigFile.getRepeatsPerQuery();
    }

}
