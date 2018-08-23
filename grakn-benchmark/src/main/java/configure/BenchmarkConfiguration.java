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
import java.util.List;

/**
 *  Contains the configuration for an execution of the benchmarking system
 */

public class BenchmarkConfiguration {

    private QueriesConfigurationFile queries;
    private List<String> schemaGraql;
    private BenchmarkConfigurationFile benchmarkConfigFile;

    public BenchmarkConfiguration(BenchmarkConfigurationFile config) throws IOException {
        this.benchmarkConfigFile = config;

        // read the queries file string and use them to load further YAML
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        queries = mapper.readValue(new File(config.getQueriesYamlFile()), QueriesConfigurationFile.class);

        try {
            schemaGraql = Files.readLines(new File(config.getSchemaFile()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName() {
        return this.benchmarkConfigFile.getName();
    }

    public List<String> getSchema() {
        return this.schemaGraql;
    }

    public List<String> getQueries() {
        return this.queries.getQueries();
    }

    public List<Integer> getConceptsToBenchmark() {
        return this.benchmarkConfigFile.getConceptsToBenchmark();
    }
}
