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

package manage;

import com.fasterxml.jackson.databind.ObjectMapper;
import configure.BenchmarkConfiguration;
import configure.BenchmarkConfigurationFile;
import executor.QueryExecutor;
import generator.DataGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


/**
 *
 */
public class BenchmarkManager {
    private DataGenerator dataGenerator;
    private QueryExecutor queryExecutor;
    private int numQueryRepetitions;
    private BenchmarkConfiguration configuration;

    public BenchmarkManager(BenchmarkConfiguration configuration, DataGenerator dataGenerator, QueryExecutor queryExecutor) {
        this.dataGenerator = dataGenerator;
        this.queryExecutor = queryExecutor;
        this.numQueryRepetitions = configuration.noQueryRepetitions();
        this.configuration = configuration;
    }

    public void run() {
        // load schema if not disabled
        if (!this.configuration.noSchemaLoad()) {
            this.dataGenerator.loadSchema();
        }

        // initialize data generation if not disabled
        if (!this.configuration.noDataGeneration()) {
            this.dataGenerator.initializeGeneration();
        }

        // run a variable dataset size or the pre-initialized one
        if (this.configuration.noDataGeneration()) {
            // count the current size of the DB
            int numConcepts = this.queryExecutor.aggregateCount();
            // only 1 point to profile at
            this.queryExecutor.processStaticQueries(numQueryRepetitions, numConcepts, "Preconfigured DB - no data gen");
        } else {
            this.runAtConcepts(this.configuration.getConceptsToBenchmark());
        }

    }

    /**
     * Given a list of database sizes to perform profiling at,
     * Populate the DB to a given size, then run the benchmark
     * @param numConceptsInRun
     */
    private void runAtConcepts(List<Integer> numConceptsInRun) {
        for (int numConcepts : numConceptsInRun) {
            this.dataGenerator.generate(numConcepts);
            this.queryExecutor.processStaticQueries(numQueryRepetitions, numConcepts);
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {

        CommandLine commandLine;
        Option keyspaceOption = Option.builder("k")
                .longOpt("keyspace")
                .required(false)
                .desc("Specific keyspace to utilize (default: `name` in config yaml")
                .type(String.class)
                .build();
        Option noDataGenerationOption = Option.builder("G")
                .longOpt("no-data-generation")
                .required(false)
                .desc("Disable data generation")
                .type(Boolean.class)
                .build();
        Option noSchemaLoadOption = Option.builder("S")
                .longOpt("no-schema-load")
                .required(false)
                .desc("Disable loading a schema")
                .type(Boolean.class)
                .build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        options.addOption(keyspaceOption);
        options.addOption(noDataGenerationOption);
        options.addOption(noSchemaLoadOption);


        String configFileName = args[0];
        ObjectMapper benchmarkConfigMapper = new ObjectMapper(new YAMLFactory());
        BenchmarkConfigurationFile configFile = benchmarkConfigMapper.readValue(
                new File(System.getProperty("user.dir") + configFileName),
                BenchmarkConfigurationFile.class);
        BenchmarkConfiguration benchmarkConfiguration = new BenchmarkConfiguration(configFile);


        // use given keyspace string if exists, otherwise use yaml file `name` tag
        String keyspace = keyspaceOption.getValue(benchmarkConfiguration.getDefaultKeyspace());

        // loading a schema file, enabled by default
        String noSchemaLoadString = noSchemaLoadOption.getValue("false");
        boolean noSchemaLoad = Boolean.parseBoolean(noSchemaLoadString);
        benchmarkConfiguration.setNoSchemaLoad(noSchemaLoad);

        // generate data true/false, else default to do generate data
        String noDataGenerationString = noDataGenerationOption.getValue("false");
        boolean noDataGeneration = Boolean.parseBoolean(noDataGenerationString);
        benchmarkConfiguration.setNoDataGeneration(noDataGeneration);

        String uri = "localhost:48555";

        DataGenerator dataGenerator;
        if (benchmarkConfiguration.noDataGeneration()) {
            // no data generation means NEITHER schema load NOR data generate
            dataGenerator = null;
        } else {
            dataGenerator = new DataGenerator(keyspace, uri, benchmarkConfiguration.getSchema());
        }

        QueryExecutor queryExecutor = new QueryExecutor(keyspace,
                                            uri,
                                            "generated_" + benchmarkConfiguration.getName(),
                                            benchmarkConfiguration.getQueries());
        BenchmarkManager manager = new BenchmarkManager(benchmarkConfiguration, dataGenerator, queryExecutor);
        manager.run();
    }
}


