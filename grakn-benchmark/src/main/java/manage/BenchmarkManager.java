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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


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

    public static void main(String[] args) throws IOException {

        Option configFileOption = Option.builder("c")
                .longOpt("config")
                .hasArg(true)
                .desc("Benchmarking YAML file (required)")
                .required(true)
                .type(String.class)
                .build();

        Option keyspaceOption = Option.builder("k")
                .longOpt("keyspace")
                .required(false)
                .hasArg(true)
                .desc("Specific keyspace to utilize (default: `name` in config yaml")
                .type(String.class)
                .build();
        Option noDataGenerationOption = Option.builder("ng")
                .longOpt("no-data-generation")
                .required(false)
                .desc("Disable data generation")
                .type(Boolean.class)
                .build();
        Option noSchemaLoadOption = Option.builder("ns")
                .longOpt("no-schema-load")
                .required(false)
                .desc("Disable loading a schema")
                .type(Boolean.class)
                .build();
        Option executionNameOption = Option.builder("n")
                .longOpt("execution-name")
                .hasArg(true)
                .required(false)
                .desc("Name for specific execution of the config file")
                .type(String.class)
                .build();
        Options options = new Options();
        options.addOption(configFileOption);
        options.addOption(keyspaceOption);
        options.addOption(noDataGenerationOption);
        options.addOption(noSchemaLoadOption);
        options.addOption(executionNameOption);
        CommandLineParser parser = new DefaultParser();
        CommandLine arguments;
        try {
            arguments = parser.parse(options, args);
        } catch (ParseException e) {
            (new HelpFormatter()).printHelp("Benchmarking options", options);
            throw new RuntimeException(e.getMessage());
        }

        String configFileName = arguments.getOptionValue("config");
        ObjectMapper benchmarkConfigMapper = new ObjectMapper(new YAMLFactory());
        BenchmarkConfigurationFile configFile = benchmarkConfigMapper.readValue(
                new File(System.getProperty("user.dir") + configFileName),
                BenchmarkConfigurationFile.class);
        BenchmarkConfiguration benchmarkConfiguration = new BenchmarkConfiguration(configFile);


        // use given keyspace string if exists, otherwise use yaml file `name` tag
        String keyspace = arguments.getOptionValue("keyspace", benchmarkConfiguration.getDefaultKeyspace());

        // loading a schema file, enabled by default
        boolean noSchemaLoad = arguments.hasOption("no-schema-load") ? true : false;
        benchmarkConfiguration.setNoSchemaLoad(noSchemaLoad);

        // generate data true/false, else default to do generate data
        boolean noDataGeneration = arguments.hasOption("no-data-generation") ? true : false;
        benchmarkConfiguration.setNoDataGeneration(noDataGeneration);

        // generate a name for this specific execution of the benchmarking
        String executionName = arguments.getOptionValue("execution-name", "");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String dateString = dateFormat.format(new Date());
        executionName = String.format("%s %s %s",dateString, benchmarkConfiguration.getName(), executionName);


        String uri = "localhost:48555";

        // no data generation means NEITHER schema load NOR data generate
        DataGenerator dataGenerator = benchmarkConfiguration.noDataGeneration() ?
                null :
                new DataGenerator(keyspace, uri, benchmarkConfiguration.getSchema());

        QueryExecutor queryExecutor = new QueryExecutor(keyspace,
                                            uri,
                                            executionName,
                                            benchmarkConfiguration.getQueries());
        BenchmarkManager manager = new BenchmarkManager(benchmarkConfiguration, dataGenerator, queryExecutor);
        manager.run();
    }
}


