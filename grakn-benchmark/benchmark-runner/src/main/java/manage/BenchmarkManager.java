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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
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
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class BenchmarkManager {
    private DataGenerator dataGenerator;
    private QueryExecutor queryExecutor;
    private int numQueryRepetitions;
    private BenchmarkConfiguration configuration;

    private static final String GRAKN_URI = "localhost:48555";

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkManager.class);

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


    public static boolean indexTemplateExists(RestClient esClient, String indexTemplateName) throws IOException {

        try {
            Request templateExistsRequest = new Request(
                    "GET",
                    "/_template/" + indexTemplateName
            );
            Response response = esClient.performRequest(templateExistsRequest);
            LOG.info("Index template `" + indexTemplateName + "` already exists");
            return true;
        } catch (ResponseException err) {
            // 404 => template does not exist yet
            LOG.error("Index template `" + indexTemplateName + "` does not exist", err);
            return false;
        }
    }

    public static void putIndexTemplateFile(RestClient esClient, File indexTemplateFile, String indexTemplateName) throws IOException {
        Request putTemplateRequest = new Request(
                "PUT",
                "/_template/" + indexTemplateName
        );
        HttpEntity entity = new FileEntity(indexTemplateFile, ContentType.APPLICATION_JSON);
        putTemplateRequest.setEntity(entity);
        Response response = esClient.performRequest(putTemplateRequest);

        LOG.info("Created index template `" + indexTemplateName + "`");
    }

    public static void main(String[] args) throws IOException {


        // TODO arguments for ElasticSearch server URI as an arugment
        String esServerHost = "localhost";
        int esServerPort = 9200;
        String esServerProtocol = "http";
        RestClientBuilder esRestClientBuilder= RestClient.builder(new HttpHost(esServerHost, esServerPort, esServerProtocol));
        esRestClientBuilder.setDefaultHeaders(new Header[]{new BasicHeader("header", "value")});
        RestClient restClient = esRestClientBuilder.build();


        String indexTemplateName = "grakn-benchmark-index-template";
        if (!indexTemplateExists(restClient, indexTemplateName)) {
            // TODO `conf` as a constant path for use all over the place
            Path confPath = Paths.get("/", "Users", "joshua", "Documents", "grakn", "grakn-benchmark", "benchmark-runner", "conf");
            Path indexTemplatePath = confPath.resolve(indexTemplateName + ".json");
            File indexTemplateFile = indexTemplatePath.toFile();
            putIndexTemplateFile(restClient, indexTemplateFile, indexTemplateName);
        }
        restClient.close();









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
        Path configFilePath = Paths.get(configFileName);

        ObjectMapper benchmarkConfigMapper = new ObjectMapper(new YAMLFactory());
        BenchmarkConfigurationFile configFile = benchmarkConfigMapper.readValue(
                configFilePath.toFile(),
                BenchmarkConfigurationFile.class);
        BenchmarkConfiguration benchmarkConfiguration = new BenchmarkConfiguration(configFilePath, configFile);


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
        executionName = String.join(" ", Arrays.asList(dateString, benchmarkConfiguration.getName(), executionName)).trim();


        // no data generation means NEITHER schema load NOR data generate
        DataGenerator dataGenerator = benchmarkConfiguration.noDataGeneration() ?
                null :
                new DataGenerator(keyspace, GRAKN_URI, benchmarkConfiguration.getSchema());

        QueryExecutor queryExecutor = new QueryExecutor(keyspace,
                                            GRAKN_URI,
                                            executionName,
                                            benchmarkConfiguration.getQueries());
        BenchmarkManager manager = new BenchmarkManager(benchmarkConfiguration, dataGenerator, queryExecutor);
        manager.run();
    }
}


